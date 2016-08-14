package org.hathitrust.htrc.tools.oclcdeduplicator


import java.io.File

import com.gilt.gfc.time.Timer
import edu.illinois.i3.scala.utils.metrics.Timer.time
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.json4s.JsonAST.{JArray, JString}
import org.json4s.jackson.JsonMethods
import org.rogach.scallop.ScallopConf

import scala.collection.TraversableOnce

/**
  * @author Boris Capitanu
  */

object Main {
  val appName = "oclc-deduplicator"

  case class DocMeta(id: String, title: String, author: String,
                     publishDate: String, format: String, oclc: String)

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)
    val metaPath = conf.metaPath()
    val outputPath = conf.outputPath()
    val numPartitions = conf.numPartitions.toOption

    val sparkConf = new SparkConf()
    sparkConf.setIfMissing("spark.master", "local[*]")
    sparkConf.setIfMissing("spark.app.name", appName)

    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    import spark.implicits._

    val sc = spark.sparkContext

    val oclcPattern = """.*?([1-9]\d+)$""".r

    val (_, elapsed) = time {
      val metaDF = spark.read.json(metaPath.toString)
      val docMeta = metaDF.select("id", "title", "author", "publishDate", "format", "oclc").as[DocMeta]

      docMeta.cache()

      val nullOclcDupes = docMeta.where($"oclc" isNull).rdd
        .groupBy {
          case DocMeta(_, title, author, publishDate, format, _) =>
            (title, author, publishDate, format)
        }.map(_._2)

      val nullOclcDupIds = nullOclcDupes.map(_.map(_.id).mkString(" "))

      val data = docMeta
        .where($"oclc" isNotNull)
        .flatMap {
          case d @ DocMeta(_, _, _, _, _, oclc) =>
            val docs = if (oclc.head == '[') {
              implicit val formats = org.json4s.DefaultFormats
              val docs: TraversableOnce[DocMeta] = for {
                JArray(arr) <- JsonMethods.parse(oclc)
                JString(v) <- arr
              } yield d.copy(oclc = v)
              docs
            } else
              Seq(d)

            docs.map {
              case d @ DocMeta(_, _, _, _, _, oclcPattern(s)) => d.copy(oclc = s)
            }.toSet
        }

      val oclcDupes = data.rdd.groupBy(_.oclc).map(_._2)
      val oclcDupIds = oclcDupes.map(_.map(_.id).mkString(" "))

      nullOclcDupIds.union(oclcDupIds).saveAsTextFile(outputPath.toString)
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

  val metaPath = opt[File]("meta",
    descr = "The path to the JSON metadata file",
    required = true,
    argName = "FILE"
  )

  val outputPath = opt[File]("output",
    descr = "Write the output to DIR",
    required = true,
    argName = "DIR"
  )

  validateFileExists(metaPath)
  verify()
}