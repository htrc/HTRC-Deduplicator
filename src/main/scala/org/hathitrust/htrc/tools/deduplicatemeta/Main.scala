package org.hathitrust.htrc.tools.deduplicatemeta


import java.io.File

import com.gilt.gfc.time.Timer
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.hathitrust.htrc.tools.deduplicatemeta.Helper.logger
import org.hathitrust.htrc.tools.pairtreehelper.PairtreeHelper
import org.hathitrust.htrc.tools.spark.errorhandling.ErrorAccumulator
import org.hathitrust.htrc.tools.spark.errorhandling.RddExtensions._
import org.rogach.scallop.{ScallopConf, ScallopOption}

import scala.io.{Codec, Source, StdIn}

/**
  * Given a list of volume IDs, this application loads the metadata information for each volume
  * (from the associated volume JSON file in the pairtree) and attempts to find "duplicates" by
  * matching volumes on a subset of the metadata attributes. This subset is currently defined in
  * the code. A future extension to this tool might allow the user to define this subset externally,
  * but it's currently not planned. The output from this tool consists of the list of "deduplicated"
  * IDs.
  *
  * @author Boris Capitanu
  */

object Main {
  val appName = "deduplicate-meta"

  def main(args: Array[String]): Unit = {
    // parse the command line arguments
    val conf = new Conf(args)
    val pairtreeRootPath = conf.pairtreeRootPath().toString
    val outputPath = conf.outputPath().toString
    val addMetadata = conf.addMetadata()
    val numPartitions = conf.numPartitions.toOption
    val htids = conf.htids.toOption match {
      case Some(file) => Source.fromFile(file).getLines().toSeq
      case None => Iterator.continually(StdIn.readLine()).takeWhile(_ != null).toSeq
    }

    // set up logging destination
    conf.sparkLog.toOption match {
      case Some(logFile) => System.setProperty("spark.logFile", logFile)
      case None =>
    }

    // set up Spark context
    val sparkConf = new SparkConf()
    sparkConf.setIfMissing("spark.master", "local[*]")
    sparkConf.setIfMissing("spark.app.name", appName)

    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    import spark.implicits._

    val sc = spark.sparkContext

    logger.info("Starting...")

    // record start time
    val t0 = System.nanoTime()

    val idsRDD = numPartitions match {
      case Some(n) => sc.parallelize(htids, n) // split input into n partitions
      case None => sc.parallelize(htids) // use default number of partitions
    }

    // read the JSON metadata for each volume in the list of IDs supplied
    val errorsVol = new ErrorAccumulator[String, String](identity)(sc)
    val errorsMeta = new ErrorAccumulator[String, String](identity)(sc)
    val metaJsonsRDD = idsRDD
      .tryMap(PairtreeHelper.getDocFromUncleanId)(errorsVol)
      .map(_.getDocumentPathPrefix(pairtreeRootPath) + ".json")
      .tryMap(Source.fromFile(_)(Codec.UTF8).mkString)(errorsMeta)

    // create a schema containing the fields we're interested in
    val metaSchema = StructType(Seq(
      StructField("volumeIdentifier", StringType, nullable = false),
      StructField("title", StringType, nullable = false),
      StructField("names", ArrayType(StringType), nullable = false),
      StructField("pubDate", StringType, nullable = false),
      StructField("enumerationChronology", StringType, nullable = false)
    ))

    // create a DataFrame from the loaded JSON records,
    // selecting only the fields defined in the schema
    val metaDF = spark.read.schema(metaSchema).json(metaJsonsRDD)
    val docMeta = metaDF.as[DocMeta]

    // group volumes together when they match on the fields specified in the "case" statement below
    val groupedDocsRDD = docMeta.rdd
      .groupBy {
        // select the fields to be used in the deduplication process
        case DocMeta(_, title, _, pubDate, enumerationChronology) =>
          (title, pubDate, enumerationChronology)
      }

    // prepare the identified duplicates for reporting
    val dupes = if (addMetadata) {
      groupedDocsRDD.map {
        case ((title, pubDate, enumerationChronology), d) =>
          val vols = d.map(_.volumeIdentifier).mkString(" ")
          s"$vols\t$title\t$pubDate\t$enumerationChronology"
      }
    } else {
      groupedDocsRDD.map(_._2.map(_.volumeIdentifier).mkString(" "))
    }

    // save the results
    dupes.saveAsTextFile(outputPath)

    if (errorsVol.nonEmpty || errorsMeta.nonEmpty)
      logger.info("Writing error report(s)...")

    // save any errors to the output folder
    if (errorsVol.nonEmpty)
      errorsVol.saveErrors(new Path(outputPath, "volume_errors.txt"), _.toString)

    if (errorsMeta.nonEmpty)
      errorsMeta.saveErrors(new Path(outputPath, "meta_errors.txt"), _.toString)

    // record elapsed time and report it
    val t1 = System.nanoTime()
    val elapsed = t1 - t0

    logger.info(f"All done in ${Timer.pretty(elapsed)}")
  }
}

// Following contains a list of all fields in the JSON schema for a volume:
//
//      val metaSchema = StructType(Seq(
//        StructField("accessProfile", StringType),
//        StructField("bibliographicFormat", StringType),
//        StructField("classification", StructType(Seq(
//          StructField("ddc", ArrayType(StringType)),
//          StructField("lcc", ArrayType(StringType))
//        ))),
//        StructField("dateCreated", DateType),
//        StructField("enumerationChronology", StringType),
//        StructField("genre", ArrayType(StringType)),
//        StructField("governmentDocument", BooleanType),
//        StructField("handleUrl", StringType),
//        StructField("hathitrustRecordNumber", StringType),
//        StructField("htBibUrl", StringType),
//        StructField("imprint", StringType),
//        StructField("isbn", ArrayType(StringType)),
//        StructField("issn", ArrayType(StringType)),
//        StructField("issuance", StringType),
//        StructField("language", StringType),
//        StructField("lastUpdateDate", StringType),
//        StructField("lccn", ArrayType(StringType)),
//        StructField("names", ArrayType(StringType)),
//        StructField("oclc", ArrayType(StringType)),
//        StructField("pubDate", StringType),
//        StructField("pubPlace", StringType),
//        StructField("rightsAttributes", StringType),
//        StructField("schemaVersion", StringType),
//        StructField("sourceInstitution", StringType),
//        StructField("sourceInstitutionRecordNumber", StringType),
//        StructField("title", StringType),
//        StructField("typeOfResource", StringType),
//        StructField("volumeIdentifier", StringType)
//      ))
//
// The "dead" code below was once useful for deduplicating volumes based on OCLC information.
// It is not removed as it may prove useful once again in the future.
//
//      docMeta.cache()
//      val oclcPattern = """.*?([1-9]\d+)$""".r
//
//      val nullOclcDupes = docMeta.where($"oclc" isNull).rdd
//        .groupBy {
//          case DocMeta(_, title, author, publishDate, format, _) =>
//            (title, publishDate)
//        }.map(_._2)
//
//      val nullOclcDupIds = nullOclcDupes.map(_.map(_.id).mkString(" "))
//
//      val data = docMeta
//        .where($"oclc" isNotNull)
//        .flatMap {
//          case d @ DocMeta(_, _, _, _, _, oclc) =>
//            val docs = if (oclc.head == '[') {
//              implicit val formats = org.json4s.DefaultFormats
//              val docs: TraversableOnce[DocMeta] = for {
//                JArray(arr) <- JsonMethods.parse(oclc)
//                JString(v) <- arr
//              } yield d.copy(oclc = v)
//              docs
//            } else
//              Seq(d)
//
//            docs.map {
//              case d @ DocMeta(_, _, _, _, _, oclcPattern(s)) => d.copy(oclc = s)
//            }.toSet
//        }
//
//      val oclcDupes = data.rdd.groupBy(d => (d.title, d.publishDate)).map(_._2)
//      val oclcDupIds = oclcDupes.map(_.map(_.id).mkString(" "))
//
//      nullOclcDupIds.union(oclcDupIds).saveAsTextFile(outputPath.toString)

/**
  * Command line argument configuration
  *
  * @param arguments The cmd line args
  */
class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val (appTitle, appVersion, appVendor) = {
    val p = getClass.getPackage
    val nameOpt = Option(p).flatMap(p => Option(p.getImplementationTitle))
    val versionOpt = Option(p).flatMap(p => Option(p.getImplementationVersion))
    val vendorOpt = Option(p).flatMap(p => Option(p.getImplementationVendor))
    (nameOpt, versionOpt, vendorOpt)
  }

  version(appVersion.flatMap(
    version => appVendor.map(
      vendor => s"${Main.appName} $version\n$vendor")).getOrElse(Main.appName))

  val sparkLog: ScallopOption[String] = opt[String]("spark-log",
    descr = "Where to write logging output from Spark to",
    argName = "FILE",
    noshort = true
  )

  val numPartitions: ScallopOption[Int] = opt[Int]("num-partitions",
    descr = "The number of partitions to split the input data into",
    required = false,
    argName = "N",
    validate = 0 <
  )

  val pairtreeRootPath: ScallopOption[File] = opt[File]("pairtree",
    descr = "The path to the paitree root hierarchy to process",
    required = true,
    argName = "DIR"
  )

  val addMetadata: ScallopOption[Boolean] = opt[Boolean]("meta",
    descr = "Flag to specify whether metadata information (title, author, pubDate) " +
      "should be included in the output",
    default = Some(false)
  )

  val outputPath: ScallopOption[File] = opt[File]("output",
    descr = "Write the output to DIR",
    required = true,
    argName = "DIR"
  )

  val htids: ScallopOption[File] = trailArg[File]("htids",
    descr = "The file containing the HT IDs to be searched (if not provided, will read from stdin)",
    required = false
  )

  validateFileExists(pairtreeRootPath)
  validateFileExists(htids)
  verify()
}