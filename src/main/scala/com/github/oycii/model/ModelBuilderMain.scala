package com.github.oycii.model

import org.apache.spark.mllib.tree.RandomForest
import com.github.oycii.utils.SparkUtils

object ModelBuilderMain extends App {
  // Модель классификации для Ирисов Фишера (https://ru.wikipedia.org/wiki/Ирисы_Фишера)
  // Загрузить данные для классификации (https://www.kaggle.com/datasets/arshid/iris-flower-dataset или https://github.com/apache/spark/blob/v3.2.3/data/mllib/iris_libsvm.txt)

  import org.apache.spark.mllib.util.MLUtils

  SparkUtils.withSparkSession("MLStreaming", spark => {
    val basePath = "data"
    val data = MLUtils.loadLibSVMFile(spark.sparkContext, basePath + "/iris_libsvm.txt")
    data.take(5).foreach(println)

    // Разбиваем данные на данные для обучения и проверки
    val splits = data.randomSplit(Array(0.3, 0.7))
    val trainingData = splits(1)
    val checkData = splits(0)
    println("count training data: " + trainingData.count)
    println("count check data: " + checkData.count)


    val numClasses = 3
    val categoricalFeaturesInfo = Map[Int, Int]()
    val numTrees = 3000
    val featureSubsetStrategy = "auto"
    val impurity = "gini"
    val maxDepth = 25
    val maxBins = 32

    val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo, numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

    println("Result for " + checkData.count)
    val predictionAndLabels = checkData.map(point => {
      println("point:" + point)
      val prediction = model.predict(point.features)
      (point.label, prediction)
    })

    //println("Pridicted " + predictionAndLabels.count)
    //println("predictionAndLabels: " + predictionAndLabels)

    predictionAndLabels.map(rec => println(rec._1 + ": " + rec._2))

    import org.apache.spark.mllib.evaluation.MulticlassMetrics

    val metrics = new MulticlassMetrics(predictionAndLabels)

    println("Confusion matrix: ")
    println(metrics.confusionMatrix)

    val accuracy = metrics.accuracy
    println()
    println("Summary statistics")
    println(s"Accuracy = $accuracy")

    println()
    val labels = metrics.labels
    labels.foreach { l => println(s"Precision($l) = " + metrics.precision(l)) }


    println()
    labels.foreach { l => println(s"Recall($l) = " + metrics.recall(l)) }


    println()
    labels.foreach { l => println(s"FPR($l) = " + metrics.falsePositiveRate(l)) }

    println()
    labels.foreach { l => println(s"F1-Scope($l) = " + metrics.fMeasure(l)) }

    println()
    println(s"Weighted precision: ${metrics.weightedPrecision}")
    println(s"Weighted recall: ${metrics.weightedRecall}")
    println(s"Weighted false positive rate: ${metrics.weightedFalsePositiveRate}")
    println(s"Weighted f1 scope: ${metrics.weightedFMeasure}")


    println()

    //model.save(spark.sparkContext, "model")
    spark.sparkContext.parallelize(Seq(model), 1).saveAsObjectFile("model")
  })

}
