package edu.neu.csye7200

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LinearSVC
import org.apache.spark.ml.feature.{StringIndexer, VectorIndexer}

object SVC {
//  val labelIndexer = new StringIndexer()
//    .setInputCol("radiant_win")
//    .setOutputCol("label")
//    .fit(Train)
//
//  val featureIndexer = new VectorIndexer()
//    .setInputCol("features")
//    .setOutputCol("indexedFeatures")
//    .setMaxCategories(4) // features with > 4 distinct values are treated as continuous
//    .fit(Train)
//
//  val svc = new LinearSVC()
//    .setLabelCol("label")
//    .setFeaturesCol("indexedFeatures")
//    .setMaxIter(10)
//    .setRegParam(0.1)
//
//  val stages = Array(
//    labelIndexer,
//    featureIndexer,
//    svc
//  )
//  val pipeline = new Pipeline().setStages(stages)
//
//  val gbtModel = pipeline.fit(Train)
//  gbtModel.write.overwrite().save("src/main/resources/svc")
}
//TP:25889
//FN:22243
//FP:18416
//TN:33437
//Precision:0.5843
//Accuracy:0.5933
//Callback:0.5379
