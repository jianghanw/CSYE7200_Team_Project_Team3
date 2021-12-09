package edu.neu.csye7200
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array, col, lit, regexp_replace}
import org.apache.spark.sql.types.IntegerType

object Main extends App{
  val spark: SparkSession = SparkSession
    .builder()
    .appName("Main")
    .master("local[*]")
    .getOrCreate()

  val team_Radiant = new Array[Int](5)
  for(i<- 0 to 4){
    print("Enter the "+i+"st hero of Radiant: ")
    team_Radiant(i)=scala.io.StdIn.readInt()
  }
  val team_Dire = new Array[Int](5)
  for(i<- 0 to 4){
    print("Enter the "+i+"st hero of Dire: ")
    team_Dire(i)=scala.io.StdIn.readInt()
  }

  val synergy_result = spark.sqlContext.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/synergy.csv")
    .sort("hero_id")
    .drop("hero_id")

  val countering_result = spark.sqlContext.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/countering.csv")

  val rowSR = synergy_result.select(array(synergy_result.columns.map(col): _*) as "row")
  val rowCR = countering_result.select(array(countering_result.columns.map(col): _*) as "row")

  val matSR = rowSR.collect.map(_.getSeq[Double](0).toArray)
  val matCR = rowSR.collect.map(_.getSeq[Double](0).toArray)

  val synergy_matrix = matSR.map(_.map(_.toDouble))
  val countering_matrix = matCR.map(_.map(_.toDouble))
  def Synergy(hero_id0: Int, hero_id1: Int, hero_id2: Int, hero_id3: Int, hero_id4: Int, hero_id128: Int, hero_id129: Int, hero_id130: Int, hero_id131: Int, hero_id132: Int): Double ={
    val synergy01 = synergy_matrix(hero_id0 - 1)(hero_id1 - 1) - synergy_matrix(hero_id128 - 1)(hero_id129 - 1)
    val synergy02 = synergy_matrix(hero_id0 - 1)(hero_id2 - 1) - synergy_matrix(hero_id128 - 1)(hero_id130 - 1)
    val synergy03 = synergy_matrix(hero_id0 - 1)(hero_id3 - 1) - synergy_matrix(hero_id128 - 1)(hero_id131 - 1)
    val synergy04 = synergy_matrix(hero_id0 - 1)(hero_id4 - 1) - synergy_matrix(hero_id128 - 1)(hero_id132 - 1)
    val synergy12 = synergy_matrix(hero_id1 - 1)(hero_id2 - 1) - synergy_matrix(hero_id129 - 1)(hero_id130 - 1)
    val synergy13 = synergy_matrix(hero_id1 - 1)(hero_id3 - 1) - synergy_matrix(hero_id129 - 1)(hero_id131 - 1)
    val synergy14 = synergy_matrix(hero_id1 - 1)(hero_id4 - 1) - synergy_matrix(hero_id129 - 1)(hero_id132 - 1)
    val synergy23 = synergy_matrix(hero_id2 - 1)(hero_id3 - 1) - synergy_matrix(hero_id130 - 1)(hero_id131 - 1)
    val synergy24 = synergy_matrix(hero_id2 - 1)(hero_id4 - 1) - synergy_matrix(hero_id130 - 1)(hero_id132 - 1)
    val synergy34 = synergy_matrix(hero_id3 - 1)(hero_id4 - 1) - synergy_matrix(hero_id131 - 1)(hero_id132 - 1)
    synergy01 + synergy02 + synergy03 + synergy04 + synergy12 + synergy13 + synergy14 + synergy23 + synergy24 + synergy34
  }

  def Countering(hero_id0: Int, hero_id1: Int, hero_id2: Int, hero_id3: Int, hero_id4: Int, hero_id128: Int, hero_id129: Int, hero_id130: Int, hero_id131: Int, hero_id132: Int): Double ={
    val countering0 = countering_matrix(hero_id0 - 1)(hero_id128 - 1) + countering_matrix(hero_id0 - 1)(hero_id129 - 1) + countering_matrix(hero_id0 - 1)(hero_id130 - 1) + countering_matrix(hero_id0 - 1)(hero_id131 - 1) + countering_matrix(hero_id0 - 1)(hero_id132 - 1)
    val countering1 = countering_matrix(hero_id1 - 1)(hero_id128 - 1) + countering_matrix(hero_id1 - 1)(hero_id129 - 1) + countering_matrix(hero_id1 - 1)(hero_id130 - 1) + countering_matrix(hero_id1 - 1)(hero_id131 - 1) + countering_matrix(hero_id1 - 1)(hero_id132 - 1)
    val countering2 = countering_matrix(hero_id2 - 1)(hero_id128 - 1) + countering_matrix(hero_id2 - 1)(hero_id129 - 1) + countering_matrix(hero_id2 - 1)(hero_id130 - 1) + countering_matrix(hero_id2 - 1)(hero_id131 - 1) + countering_matrix(hero_id2 - 1)(hero_id132 - 1)
    val countering3 = countering_matrix(hero_id3 - 1)(hero_id128 - 1) + countering_matrix(hero_id3 - 1)(hero_id129 - 1) + countering_matrix(hero_id3 - 1)(hero_id130 - 1) + countering_matrix(hero_id3 - 1)(hero_id131 - 1) + countering_matrix(hero_id3 - 1)(hero_id132 - 1)
    val countering4 = countering_matrix(hero_id4 - 1)(hero_id128 - 1) + countering_matrix(hero_id4 - 1)(hero_id129 - 1) + countering_matrix(hero_id4 - 1)(hero_id130 - 1) + countering_matrix(hero_id4 - 1)(hero_id131 - 1) + countering_matrix(hero_id4 - 1)(hero_id132 - 1)
    countering0 + countering1 + countering2 + countering3 + countering4 - 12.5
  }

  val synergy=Synergy(team_Radiant(0),team_Radiant(1),team_Radiant(2),team_Radiant(3),team_Radiant(4),team_Dire(0),team_Dire(1),team_Dire(2),team_Dire(3),team_Dire(4))
  val countering=Countering(team_Radiant(0),team_Radiant(1),team_Radiant(2),team_Radiant(3),team_Radiant(4),team_Dire(0),team_Dire(1),team_Dire(2),team_Dire(3),team_Dire(4))

  val player_template = spark.sqlContext.read
    .option("header", "true")
    .option("charset", "UTF8")
    .option("inferSchema", "true")
    .csv("src/main/resources/player_template.csv")
  val hero_id0=team_Radiant(0)
  val hero_id1=team_Radiant(1)
  val hero_id2=team_Radiant(2)
  val hero_id3=team_Radiant(3)
  val hero_id4=team_Radiant(4)
  val hero_id128=team_Dire(0)
  val hero_id129=team_Dire(1)
  val hero_id130=team_Dire(2)
  val hero_id131=team_Dire(3)
  val hero_id132=team_Dire(4)
  val player_result=player_template
    .withColumn(s"$hero_id0",lit(1))
    .withColumn(s"$hero_id1",lit(1))
    .withColumn(s"$hero_id2",lit(1))
    .withColumn(s"$hero_id3",lit(1))
    .withColumn(s"$hero_id4",lit(1))
    .withColumn(s"$hero_id128",lit(1))
    .withColumn(s"$hero_id129",lit(1))
    .withColumn(s"$hero_id130",lit(1))
    .withColumn(s"$hero_id131",lit(1))
    .withColumn(s"$hero_id132",lit(1))
    .withColumn("Synergy",lit(synergy))
    .withColumn("Countering",lit(countering))
  val assembler: VectorAssembler = new VectorAssembler()
    .setInputCols(Array(
      "1", "2", "3", "4", "5", "6", "7", "8", "9", "10",
      "11", "12", "13", "14", "15", "16", "17", "18", "19", "20",
      "21", "22", "23", "24", "25", "26", "27", "28", "29", "30",
      "31", "32", "33", "34", "35", "36", "37", "38", "39", "30",
      "41", "42", "43", "44", "45", "46", "47", "48", "49", "50",
      "51", "52", "53", "54", "55", "56", "57", "58", "59", "60",
      "61", "62", "63", "64", "65", "66", "67", "68", "69", "70",
      "71", "72", "73", "74", "75", "76", "77", "78", "79", "80",
      "81", "82", "83", "84", "85", "86", "87", "88", "89", "90",
      "81", "82", "83", "84", "85", "86", "87", "88", "89", "90",
      "91", "92", "93", "94", "95", "96", "97", "98", "99", "100",
      "101", "102", "103", "104", "105", "106", "107", "108", "109", "110",
      "111", "112", "113", "114", "115", "116", "117", "118", "119", "120",
      "121", "122", "123", "124", "125", "126", "127", "128", "129", "130",
      "131", "132", "133", "134", "135", "136", "137", "138", "139", "140",
      "141", "142", "143", "144", "145", "146", "147", "148", "149", "150",
      "161", "162", "163", "164", "165", "166", "167", "168", "169", "160",
      "171", "172", "173", "174", "175", "176", "177", "178", "179", "180",
      "181", "182", "183", "184", "185", "186", "187", "188", "189", "190",
      "191", "192", "193", "194", "195", "196", "197", "198", "199", "200",
      "201", "202", "203", "204", "205", "206", "207", "208", "209", "210",
      "211", "212", "213", "214", "215", "216", "217", "218", "219", "220",
      "221", "222", "223", "224", "225", "226",
      "Synergy", "Countering", "Offset"))
    .setOutputCol("features")
  val final_result=assembler.transform(player_result)
  val sameModel = PipelineModel.load("src/main/resources/svc")
  val predictions = sameModel.transform(final_result)
  if(predictions.select("prediction").first().getDouble(0)==1.0){
    println("radiant win!")
  }else{
    println("dire win!")
  }
}
