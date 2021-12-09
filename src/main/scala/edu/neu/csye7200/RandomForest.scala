package edu.neu.csye7200

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.{StringIndexer, VectorIndexer}

object RandomForest {
//  val labelIndexer = new StringIndexer()
//    .setInputCol("radiant_win")
//    .setOutputCol("label")
//    .fit(Train)
//
//  // Automatically identify categorical features, and index them.
//  val featureIndexer = new VectorIndexer()
//    .setInputCol("features")
//    .setOutputCol("indexedFeatures")
//    .setMaxCategories(4) // features with > 4 distinct values are treated as continuous
//    .fit(Train)
//
//  val rf = new RandomForestClassifier()
//    .setLabelCol("label")
//    .setFeaturesCol("indexedFeatures")
//
//  val stages = Array(
//    labelIndexer,
//    featureIndexer,
//    rf
//  )
//  val pipeline = new Pipeline().setStages(stages)
//
//  val dtModel = pipeline.fit(Train)
//  dtModel.write.overwrite().save("src/main/resources/rf")
}
//TP:23170
//FN:24962
//FP:16263
//TN:35590
//Precision:0.5876
//Accuracy:0.5877
//Callback:0.4814
