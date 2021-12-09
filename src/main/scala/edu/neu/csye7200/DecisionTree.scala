package edu.neu.csye7200

import org.apache.commons.math3.fitting.leastsquares.LeastSquaresFactory.model
import org.apache.spark.SparkConf
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier, LinearSVC, LinearSVCModel}
import org.apache.spark.ml.evaluation.{MulticlassClassificationEvaluator, RegressionEvaluator}
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.feature.{StandardScaler, StandardScalerModel, StringIndexer, VectorAssembler, VectorIndexer}

object DecisionTree{
//    val labelIndexer = new StringIndexer()
//      .setInputCol("radiant_win")
//      .setOutputCol("label")
//      .fit(DataProcess.Train)
//
//    // Automatically identify categorical features, and index them.
//    val featureIndexer = new VectorIndexer()
//      .setInputCol("features")
//      .setOutputCol("indexedFeatures")
//      .fit(DataProcess.Train)
//
//    val dt: DecisionTreeClassifier = new DecisionTreeClassifier()
//      .setLabelCol("label")
//      .setFeaturesCol("indexedFeatures")
//
//    val stages = Array(
//      labelIndexer,
//      featureIndexer,
//      dt
//    )
//    val pipeline = new Pipeline().setStages(stages)
//
//    val dtModel = pipeline.fit(DataProcess.Train)
//    dtModel.write.overwrite().save("src/main/resources/dt")
}
//  TP:24905
//  FN:23227
//  FP:17455
//  TN:34398
//  Precision:0.5879
//  Accuracy:0.5931
//  Callback:0.5174
