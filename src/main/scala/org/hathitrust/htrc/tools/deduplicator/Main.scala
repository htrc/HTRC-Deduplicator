package org.hathitrust.htrc.tools.deduplicator


import java.io.File

import com.gilt.gfc.time.Timer
import edu.illinois.i3.scala.utils.metrics.Timer.time
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.hathitrust.htrc.tools.pairtreehelper.PairtreeHelper
import org.rogach.scallop.ScallopConf

import scala.io.{Codec, Source, StdIn}

/**
  * @author Boris Capitanu
  */

object Main {
  val appName = "deduplicator"

  case class DocMeta(volumeIdentifier: String, title: String, names: String,
                     pubDate: String, enumerationChronology: String, oclc: String)

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)
    val pairtreeRootPath = conf.pairtreeRootPath()
    val outputPath = conf.outputPath()
    val numPartitions = conf.numPartitions.toOption
    val htids = conf.htids.toOption match {
      case Some(file) => Source.fromFile(file).getLines().toSeq
      case None => Iterator.continually(StdIn.readLine()).takeWhile(_ != null).toSeq
    }

    val sparkConf = new SparkConf()
    sparkConf.setIfMissing("spark.master", "local[*]")
    sparkConf.setIfMissing("spark.app.name", appName)

    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    import spark.implicits._

    val sc = spark.sparkContext

    val (_, elapsed) = time {
      val ids = numPartitions match {
        case Some(n) => sc.parallelize(htids, n)  // split input into n partitions
        case None => sc.parallelize(htids)        // use default number of partitions
      }

      val metaJsons = ids
        .map(PairtreeHelper.getDocFromUncleanId)
        .map(d => new File(pairtreeRootPath, s"${d.getDocumentPathPrefix}.json"))
        .filter(_.exists())
        .map(_.toString)
        .collect()
        //.map(Source.fromFile(_)(Codec.UTF8).mkString)

      //val metaDF = spark.read.json(metaJsons)
      val metaDF = spark.read.json(metaJsons: _*)

      val docMeta = metaDF.select(
        "volumeIdentifier", "title", "names",
        "pubDate", "enumerationChronology", "oclc")
        .as[DocMeta]

      docMeta.rdd
        .groupBy {
          case DocMeta(_, title, _, pubDate, enumerationChronology, _) =>
            (title, pubDate, enumerationChronology)
        }
        .map(_._2)
        .map(_.map(_.volumeIdentifier).mkString(" "))
        .saveAsTextFile(outputPath.toString)

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
    }

    println(f"All done in ${Timer.pretty(elapsed*1e6.toLong)}")
  }

}

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

  version(appTitle.flatMap(
    name => appVersion.flatMap(
      version => appVendor.map(
        vendor => s"$name $version\n$vendor"))).getOrElse(Main.appName))

  val numPartitions = opt[Int]("num-partitions",
    descr = "The number of partitions to split the input data into",
    required = false,
    argName = "N",
    validate = 0<
  )

  val pairtreeRootPath = opt[File]("pairtree",
    descr = "The path to the paitree root hierarchy to process",
    required = true,
    argName = "DIR"
  )

  val outputPath = opt[File]("output",
    descr = "Write the output to DIR",
    required = true,
    argName = "DIR"
  )

  val htids = trailArg[File]("htids",
    descr = "The file containing the HT IDs to be searched (if not provided, will read from stdin)",
    required = false
  )

  validateFileExists(pairtreeRootPath)
  validateFileExists(htids)
  verify()
}