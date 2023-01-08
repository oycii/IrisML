package com.github.oycii.model

import com.github.oycii.utils.SparkUtils
import org.apache.spark.ml.PipelineModel
import org.apache.spark.mllib.tree.model.RandomForestModel

object ModelLoadMain extends App {
  SparkUtils.withSparkSession("MLStreaming", spark => {
    spark.sparkContext.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")
    spark.sparkContext.hadoopConfiguration.set("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive", "true")
    //val model = spark.sparkContext.objectFile[RandomForestModel]("model").first()
    val sameModel = spark.sparkContext.objectFile[RandomForestModel]("model").first()
    val vector = org.apache.spark.mllib.linalg.Vectors.dense(5.1, 3.5, 1.4, 0.2)
    println("result iris: " + sameModel.predict(vector))
  })
}
