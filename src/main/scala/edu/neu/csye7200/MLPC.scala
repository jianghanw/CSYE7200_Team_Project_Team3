package edu.neu.csye7200

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.feature.{StringIndexer, VectorIndexer}

object MLPC {
//  val labelIndexer = new StringIndexer()
//    .setInputCol("radiant_win")
//    .setOutputCol("label")
//    .fit(Train)
//
//  val featureIndexer = new VectorIndexer()
//    .setInputCol("features")
//    .setOutputCol("indexedFeatures")
//    .fit(Train)
//
//  val layers = Array[Int](229, 5, 2)
//  val mlpc = new MultilayerPerceptronClassifier()
//    .setLayers(layers)
//    .setBlockSize(128)
//    .setSeed(1234L)
//    .setMaxIter(100)
//
//  val stages = Array(
//    labelIndexer,
//    featureIndexer,
//    mlpc
//  )
//  val pipeline = new Pipeline().setStages(stages)
//
//  val gbtModel = pipeline.fit(Train)
//  gbtModel.write.overwrite().save("src/main/resources/mlpc")
}
//TP:25500
//FN:22632
//FP:20318
//TN:31535
//Precision:0.5564
//Accuracy:0.5704
//Callback:0.5298
