
import org.apache.spark.sql.functions.{bround, col}
import org.apache.spark.sql.SparkSession


object data_processing extends App {
  System.setProperty("hadoop.home.dir", "C:/Program Files/hadoop-3.2.0")
  val spark: SparkSession = SparkSession
    .builder()
    .appName("ProcessData")
    .master("local[*]")
    .getOrCreate()

  val matchInfo = spark.read.format("csv")
    .option("header", "true")
    .option("inferschema", "true")
    .load("src/main/resources/match.csv")


  val data = matchInfo.select("match_id","duration","radiant_win").withColumn("radiant_win", col("radiant_win").cast("int"))

  data.show()

  val playerInfo = spark.read.format("csv")
    .option("header", "true")
    .option("inferschema", "true")
    .load("src/main/resources/players.csv")

  val playerData = playerInfo.select("match_id","hero_id","player_slot","gold_per_min","xp_per_min","kills","deaths","assists")
  val kdaCal = (col("kills")+col("assists"))/col("deaths")
  val kdaCom = playerData.withColumn("KDA",bround(kdaCal,2)).drop("kills","deaths","assists")
  kdaCom.show()
}
