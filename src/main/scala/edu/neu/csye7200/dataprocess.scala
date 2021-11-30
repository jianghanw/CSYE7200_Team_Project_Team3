package edu.neu.csye7200

import edu.neu.csye7200.dataprocess.spark.sqlContext
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, Dataset, SparkSession}

object dataprocess extends App {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("ProcessData")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  final case class heronamesFormat(
                                    hero_id: Int,
                                    localized_name: String,
                                  )

  val heronames = sqlContext.read
    .option("header", "true")
    .option("charset", "UTF8")
    .csv("src/main/resources/hero_names.csv")
    .select("hero_id", "localized_name")
    .withColumn("hero_id",col("hero_id").cast(IntegerType))
    .as[heronamesFormat]
  heronames.show()

  final case class playersFormat(
                                  match_id: Int,
                                  hero_id: Int,
                                  player_slot: Int,
                                )

  val players = sqlContext.read
    .option("header", "true")
    .option("charset", "UTF8")
    .csv("src/main/resources/players.csv")
    .select("match_id", "hero_id", "player_slot")
    .withColumn("match_id",col("match_id").cast(IntegerType))
    .withColumn("hero_id",col("hero_id").cast(IntegerType))
    .withColumn("player_slot",col("player_slot").cast(IntegerType))
    .as[playersFormat]
  players.show()

  final case class matchFormat(
                                match_id: Int,
                                radiant_win: Boolean,
                              )

  val matches = sqlContext.read
    .option("header", "true")
    .option("charset", "UTF8")
    .csv("src/main/resources/match.csv")
    .select("match_id",  "radiant_win")
    .withColumn("match_id",col("match_id").cast(IntegerType))
    .withColumn("radiant_win",col("radiant_win").cast(BooleanType))
    .as[matchFormat]



  final case class resultFormat(
                                 match_id: Int,
                                 radiant_win: Boolean,
                                 hero_id: Int,
                                 player_slot: Int,
                               )

  val result = matches.join(players, "match_id").as[resultFormat]
  result.show()

  result.createOrReplaceGlobalTempView("result")

  final case class winRateFormat(
                                 hero_id: Int,
                                 win_R: Double,
                                 win_D: Double,
                               )
}


