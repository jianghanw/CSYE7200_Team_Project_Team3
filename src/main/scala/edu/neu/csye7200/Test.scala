package edu.neu.csye7200

import edu.neu.csye7200.Test.spark.sqlContext
import org.apache.spark.sql.{Column, Dataset, SparkSession}

object Test extends App {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("ProcessData")
    .master("local[*]")
    .getOrCreate()
  import spark.implicits._
  final case class heronamesFormat (
                                     name: String,
                                     hero_id: String,
                                     localized_name: String,
                                   )
  val path = "src/main/resources/hero_names.csv"
  val heronames = sqlContext.read
    .option("header", "true")
    .option("charset", "UTF8")
    .csv(path)
    .as[heronamesFormat]

  heronames.take(5).map(println)
}
