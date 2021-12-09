package edu.neu.csye7200


import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.GBTClassifier
import org.apache.spark.ml.feature.{StringIndexer, VectorIndexer}

object GBT {
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
//  val gbt = new GBTClassifier()
//    .setLabelCol("label")
//    .setFeaturesCol("indexedFeatures")
//    .setMaxIter(10)
//    .setFeatureSubsetStrategy("auto")
//
//  val stages = Array(
//    labelIndexer,
//    featureIndexer,
//    gbt
//  )
//  val pipeline = new Pipeline().setStages(stages)
//
//  val gbtModel = pipeline.fit(Train)
//  gbtModel.write.overwrite().save("src/main/resources/gbt")
}
//TP:25373
//FN:22759
//FP:17922
//TN:33931
//Precision:0.5860
//Accuracy:0.5931
//Callback:0.5347

