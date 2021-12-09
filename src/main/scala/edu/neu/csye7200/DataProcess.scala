package edu.neu.csye7200

import edu.neu.csye7200.DataProcess.spark.sqlContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{DecisionTreeClassifier, GBTClassifier, LinearSVC, MultilayerPerceptronClassifier, NaiveBayes, RandomForestClassifier}
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature.{LabeledPoint, StandardScaler, StandardScalerModel, StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.sql.functions.{array, bround, col, collect_list, count, desc, expr, first, lit, sum, udf, when}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}
import scala.language.postfixOps

object DataProcess{

  val spark: SparkSession = SparkSession
    .builder()
    .appName("DataProcess")
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
    .withColumn("hero_id", col("hero_id").cast(IntegerType))
    .as[heronamesFormat]

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
    .withColumn("match_id", col("match_id").cast(IntegerType))
    .withColumn("hero_id", col("hero_id").cast(IntegerType))
    .withColumn("player_slot", col("player_slot").cast(IntegerType))
    .as[playersFormat]

  final case class matchFormat(
                                match_id: Int,
                                radiant_win: Boolean,
                              )

  val matches = sqlContext.read
    .option("header", "true")
    .option("charset", "UTF8")
    .csv("src/main/resources/match.csv")
    .select("match_id", "radiant_win")
    .withColumn("match_id", col("match_id").cast(IntegerType))
    .withColumn("radiant_win", col("radiant_win").cast(BooleanType))
    .as[matchFormat]

  final case class resultFormat(
                                 match_id: Int,
                                 radiant_win: Boolean,
                                 hero_id: Int,
                                 player_slot: Int,
                               )

  val tmp_result = matches
    .join(players, "match_id")
  val hero0_result = tmp_result
    .select("match_id")
    .where("hero_id=0")
    .join(tmp_result, "match_id")
  val result = tmp_result.except(hero0_result)
  result.sort("hero_id")
  result.createOrReplaceTempView("result")

  final case class winRateFormat(
                                  hero_id: Int,
                                  win_R: Double,
                                  win_D: Double,
                                )

  val tmpDF = result
    .groupBy("hero_id")
    .count()

  val tmpDF1 = result
    .groupBy("hero_id")
    .agg(sum(when($"radiant_win" === true, 1).otherwise(0)).as("num_wins"))

  val mergeDF = tmpDF.join(tmpDF1, "hero_id")

  val win_R_cal = bround(col("num_wins") / col("count"), 3)

  val win_D_cal = bround((col("count") - col("num_wins")) / col("count"), 3)

  val winRateDF = mergeDF.withColumn("win_R", win_R_cal)
    .withColumn("win_D", win_D_cal)
    .drop("count", "num_wins")

  val winRateDS = winRateDF.as[winRateFormat]

  val winteam_result = sqlContext.sql("SELECT MATCH_ID,HERO_ID FROM RESULT WHERE CASE RADIANT_WIN WHEN TRUE THEN PLAYER_SLOT>=0 AND PLAYER_SLOT<=4 ELSE PLAYER_SLOT>=128 AND PLAYER_SLOT<=132 END")
  val loseteam_result = sqlContext.sql("SELECT MATCH_ID,HERO_ID FROM RESULT WHERE CASE RADIANT_WIN WHEN FALSE THEN PLAYER_SLOT>=0 AND PLAYER_SLOT<=4 ELSE PLAYER_SLOT>=128 AND PLAYER_SLOT<=132 END")


  //  val ColumnSeparator = ","
  //  val synergyWriter = new FileWriter("src/main/resources/synergy.csv", true)
  //  val counteringWriter = new FileWriter("src/main/resources/countering.csv", true)
  //
  //  for(i<-1 to 113){
  //    val hero_id = i
  //    val query = s"hero_id=$hero_id"
  //    val tmp_winmatchid = winteam_result
  //      .select("match_id")
  //      .where(query)
  //
  //    val tmp_losematchid = loseteam_result
  //      .select("match_id")
  //      .where(query)
  //
  //    val winTM_id = winteam_result
  //      .join(tmp_winmatchid, winteam_result("match_id") === tmp_winmatchid("match_id"), "inner")
  //      .select("HERO_ID")
  //    val loseTM_id = loseteam_result
  //      .join(tmp_losematchid, loseteam_result("match_id") === tmp_losematchid("match_id"), "inner")
  //      .select("HERO_ID")
  //
  //    val winTM_num = winTM_id
  //      .groupBy("hero_id")
  //      .count()
  //      .withColumnRenamed("count", "win")
  //
  //    val loseTM_num = loseTM_id.groupBy("hero_id")
  //      .count()
  //      .withColumnRenamed("count", "lose")
  //
  //    val lose_id = result
  //      .join(tmp_losematchid, result("match_id") === tmp_losematchid("match_id"), "inner")
  //      .select("HERO_ID")
  //
  //    val win_id = result
  //      .join(tmp_winmatchid, result("match_id") === tmp_winmatchid("match_id"), "inner")
  //      .select("HERO_ID")
  //
  //    val lose_num = lose_id
  //      .groupBy("hero_id")
  //      .count()
  //      .withColumnRenamed("count", "lose_all")
  //
  //    val win_num = win_id
  //      .groupBy("hero_id")
  //      .count()
  //      .withColumnRenamed("count", "win_all")
  //
  //    val winOP_num = win_num
  //      .join(winTM_num, "hero_id")
  //      .withColumn("win_op", $"win_all" - $"win")
  //
  //    val loseOP_num = lose_num
  //      .join(loseTM_num, "hero_id")
  //      .withColumn("lose_op", $"lose_all" - $"lose")
  //
  //    val synergy = winTM_num
  //      .join(loseTM_num, "hero_id")
  //      .withColumn("synergy", $"win" / ($"lose" + $"win"))
  //      .select("hero_id", "synergy")
  //      .union(Seq((24, 0.5)).toDF)
  //      .union(Seq((108, 0.5)).toDF)
  //      .union(Seq((113, 0.5)).toDF)
  //      .sort("hero_id")
  //
  //    val countering = winOP_num
  //      .join(loseOP_num, "hero_id")
  //      .withColumn("countering", $"win_op" / ($"lose_op" + $"win_op"))
  //      .select("hero_id", "countering")
  //      .na.fill(0.5)
  //      .union(Seq((24, 0.5)).toDF)
  //      .union(Seq((108, 0.5)).toDF)
  //      .union(Seq((113, 0.5)).toDF)
  //    val tmpSynergySeq=synergy.select("synergy").map(_.getDouble(0)).collect.toSeq
  //    synergyWriter.write(tmpSynergySeq.mkString(ColumnSeparator)+"\n")
  //    val tmpCounteringSeq=countering.select("countering").map(_.getDouble(0)).collect.toSeq
  //    counteringWriter.write(tmpCounteringSeq.mkString(ColumnSeparator)+"\n")
  //  }
  //  synergyWriter.close()
  //  counteringWriter.close()


  val synergy_result = sqlContext.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/synergy.csv")
    .sort("hero_id")
    .drop("hero_id")

  val countering_result = sqlContext.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/countering.csv")

  val rowSR = synergy_result.select(array(synergy_result.columns.map(col): _*) as "row")
  val rowCR = countering_result.select(array(countering_result.columns.map(col): _*) as "row")

  val matSR = rowSR.collect.map(_.getSeq[Double](0).toArray)
  val matCR = rowSR.collect.map(_.getSeq[Double](0).toArray)

  val synergy_matrix = matSR.map(_.map(_.toDouble))
  val countering_matrix = matCR.map(_.map(_.toDouble))

  val cal = udf((hero_id: Int, player_slot: Int) => {
    if (player_slot >= 0 && player_slot <= 4) {
      hero_id
    } else {
      hero_id + 113
    }
  })

  val process1_result = result
    .withColumn("cal_hero_id", cal(col("hero_id"), col("player_slot")))
    .select("match_id", "radiant_win", "cal_hero_id")

  val process2_result = process1_result
    .groupBy("match_id")
    .pivot("cal_hero_id")
    .count().na.fill(0)
    .withColumn("offset", lit(1))
    .withColumn("24", lit(0))
    .withColumn("137", lit(0))
    .withColumn("108", lit(0))
    .withColumn("113", lit(0))
    .withColumn("221", lit(0))
    .withColumn("226", lit(0))

  process2_result.show(5)

  val process3_result = result
    .select("match_id", "radiant_win")
    .dropDuplicates("match_id")
    .join(process2_result, "match_id")


  val player_slot_result = result
    .groupBy("match_id")
    .pivot("player_slot")
    .agg(first("hero_id"))

  val synergy = udf((hero_id0: Int, hero_id1: Int, hero_id2: Int, hero_id3: Int, hero_id4: Int, hero_id128: Int, hero_id129: Int, hero_id130: Int, hero_id131: Int, hero_id132: Int) => {
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
  })

  val countering = udf((hero_id0: Int, hero_id1: Int, hero_id2: Int, hero_id3: Int, hero_id4: Int, hero_id128: Int, hero_id129: Int, hero_id130: Int, hero_id131: Int, hero_id132: Int) => {
    val countering0 = countering_matrix(hero_id0 - 1)(hero_id128 - 1) + countering_matrix(hero_id0 - 1)(hero_id129 - 1) + countering_matrix(hero_id0 - 1)(hero_id130 - 1) + countering_matrix(hero_id0 - 1)(hero_id131 - 1) + countering_matrix(hero_id0 - 1)(hero_id132 - 1)
    val countering1 = countering_matrix(hero_id1 - 1)(hero_id128 - 1) + countering_matrix(hero_id1 - 1)(hero_id129 - 1) + countering_matrix(hero_id1 - 1)(hero_id130 - 1) + countering_matrix(hero_id1 - 1)(hero_id131 - 1) + countering_matrix(hero_id1 - 1)(hero_id132 - 1)
    val countering2 = countering_matrix(hero_id2 - 1)(hero_id128 - 1) + countering_matrix(hero_id2 - 1)(hero_id129 - 1) + countering_matrix(hero_id2 - 1)(hero_id130 - 1) + countering_matrix(hero_id2 - 1)(hero_id131 - 1) + countering_matrix(hero_id2 - 1)(hero_id132 - 1)
    val countering3 = countering_matrix(hero_id3 - 1)(hero_id128 - 1) + countering_matrix(hero_id3 - 1)(hero_id129 - 1) + countering_matrix(hero_id3 - 1)(hero_id130 - 1) + countering_matrix(hero_id3 - 1)(hero_id131 - 1) + countering_matrix(hero_id3 - 1)(hero_id132 - 1)
    val countering4 = countering_matrix(hero_id4 - 1)(hero_id128 - 1) + countering_matrix(hero_id4 - 1)(hero_id129 - 1) + countering_matrix(hero_id4 - 1)(hero_id130 - 1) + countering_matrix(hero_id4 - 1)(hero_id131 - 1) + countering_matrix(hero_id4 - 1)(hero_id132 - 1)
    countering0 + countering1 + countering2 + countering3 + countering4 - 12.5
  })

  val process1_player_slot_result = player_slot_result
    .withColumn("synergy", synergy(col("0"), col("1"), col("2"), col("3"), col("4"), col("128"), col("129"), col("130"), col("131"), col("132")))
    .withColumn("countering", countering(col("0"), col("1"), col("2"), col("3"), col("4"), col("128"), col("129"), col("130"), col("131"), col("132")))
    .select("match_id", "synergy", "countering")

  val final_result = process3_result
    .join(process1_player_slot_result, "match_id")
    .withColumn("radiant_win", col("radiant_win").cast(DoubleType))

  final_result.show(5)
//
//  //   Test data Part
  val test_player = sqlContext.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/test_player.csv")
    .select("match_id", "hero_id", "player_slot")

  val test_labels = sqlContext.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/test_labels.csv")
    .withColumn("radiant_win", col("radiant_win").cast(BooleanType))

  val test_tmp = test_player
    .join(test_labels, "match_id")
  val hero0_test = test_tmp
    .select("match_id")
    .where("hero_id=0")
    .join(test_tmp, "match_id")

  val test_result=test_tmp.except(hero0_test)

  val process1_test = test_player
    .join(test_labels, "match_id")
    .withColumn("cal_hero_id", cal(col("hero_id"), col("player_slot")))
    .select("match_id", "radiant_win", "cal_hero_id")

  val process2_test = process1_test
    .groupBy("match_id")
    .pivot("cal_hero_id")
    .count().na.fill(0)
    .withColumn("offset", lit(1))
    .withColumn("24", lit(0))
    .withColumn("137", lit(0))
    .withColumn("108", lit(0))
    .withColumn("113", lit(0))
    .withColumn("221", lit(0))
    .withColumn("226", lit(0))

  val process3_test = test_labels
    .join(process2_test, "match_id")

  val player_slot_test = test_result
    .groupBy("match_id")
    .pivot("player_slot")
    .agg(first("hero_id"))

  val player_slot_test1 = player_slot_test
    .withColumn("synergy", synergy(col("0"), col("1"), col("2"), col("3"), col("4"), col("128"), col("129"), col("130"), col("131"), col("132")))
    .withColumn("countering", countering(col("0"), col("1"), col("2"), col("3"), col("4"), col("128"), col("129"), col("130"), col("131"), col("132")))
    .select("match_id", "synergy", "countering")

  val final_test = process3_test
    .join(player_slot_test1, "match_id")
    .withColumn("radiant_win", col("radiant_win").cast(DoubleType))

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
      "synergy", "countering", "offset"))
    .setOutputCol("features")

  val Train = assembler.transform(final_result)
  val Test = assembler.transform(final_test)

//  val labelIndexer = new StringIndexer()
//    .setInputCol("radiant_win")
//    .setOutputCol("label")
//    .fit(DataProcess.Train)
//
//  // Automatically identify categorical features, and index them.
//  val featureIndexer = new VectorIndexer()
//    .setInputCol("features")
//    .setOutputCol("indexedFeatures")
//    .fit(DataProcess.Train)
//
//  val dt: DecisionTreeClassifier = new DecisionTreeClassifier()
//    .setLabelCol("label")
//    .setFeaturesCol("indexedFeatures")
//
//  val stages = Array(
//    labelIndexer,
//    featureIndexer,
//    dt
//  )
//  val pipeline = new Pipeline().setStages(stages)
//
//  val dtModel = pipeline.fit(DataProcess.Train)
//  dtModel.write.overwrite().save("src/main/resources/dt")

//  val sameModel = PipelineModel.load("src/main/resources/svc")
//  val predictions = sameModel.transform(Test)

//  val evaluator = new BinaryClassificationEvaluator().setLabelCol("label").setRawPredictionCol("rawPrediction")
//
//  val accuracy = evaluator.evaluate(Test_transform)

//  val TP=predictions.where("prediction=1.0").where("label=1.0").count().toInt
//  val FN=predictions.where("prediction=0.0").where("label=1.0").count().toInt
//  val FP=predictions.where("prediction=1.0").where("label=0.0").count().toInt
//  val TN=predictions.where("prediction=0.0").where("label=0.0").count().toInt

//  println("TP:"+TP)
//  println("FN:"+FN)
//  println("FP:"+FP)
//  println("TN:"+TN)
//  val precision:Double=TP/(TP+FP)
//  val accuracy:Double=(TP+TN)/(TP+FN+FP+TN)
//  val callback:Double=TP/(TP+FN)
//  println("Precision:"+precision)
//  println("Accuracy:"+accuracy)
//  println("Callback:"+callback)
}

