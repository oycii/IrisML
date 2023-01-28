package com.github.oycii.ml.streaming

import com.github.oycii.entity.Iris
import com.github.oycii.ml.config.{AppConfig, AppConfigParser}
import com.github.oycii.model.Consts
import com.github.oycii.utils.SparkUtils
import io.circe
import io.circe.generic.auto._
import io.circe.parser.decode
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.streaming._
import io.circe.syntax._
import scala.language.implicitConversions

object PredictStructuredStreamingMain {

  def predictIris(sepalLength: Double, sepalWidth: Double, petalLength: Double, petalWidth: Double)(implicit model:RandomForestModel): String = {
    val vector = org.apache.spark.mllib.linalg.Vectors.dense(sepalLength, sepalWidth, petalLength, petalWidth)
    Consts.irisNames(model.predict(vector).toInt)
  }

  def toJson(sepalLength: Double, sepalWidth: Double, petalLength: Double, petalWidth: Double, irisName: String): String = {
    val iris = Iris(sepalLength, sepalWidth, petalLength, petalWidth, irisName)
    iris.asJson.toString()
  }

  def predictUdf(model: RandomForestModel) = udf {(sepalLength: Double, sepalWidth: Double, petalLength: Double, petalWidth: Double) =>
    implicit val m = model
    predictIris(sepalLength, sepalWidth, petalLength, petalWidth)
  }

  val toJsonUdf = udf {(sepalLength: Double, sepalWidth: Double, petalLength: Double, petalWidth: Double, irisName: String) =>
    toJson(sepalLength, sepalWidth, petalLength, petalWidth, irisName)
  }

  def ind(fieldName: String)(implicit schema: StructType): Int = {
    schema.fieldIndex(fieldName)
  }

  def main(args: Array[String]): Unit = {

    println(Consts.irisNames(0))
    val argsParser = AppConfigParser.parser()
    val appConfig = argsParser.parse(args, AppConfig())

    val cfg: AppConfig = appConfig match {
      case Some(value) => value
      case None =>
        throw new Exception("Usage: MLStreaming <path-to-model> <bootstrap-servers> <groupId> <input-topic> <prediction-topic>")
    }

    SparkUtils.withSparkSession("MLStreaming", spark => {
      val streamingContext = new StreamingContext(spark.sparkContext, Seconds(1))
      val sparkContext = streamingContext.sparkContext
      sparkContext.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive","true")
      sparkContext.hadoopConfiguration.set("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive","true")

      val model = spark.sparkContext.objectFile[RandomForestModel](cfg.pathToModel).first()

      val dataStreamReader = spark
        .readStream
        .format("kafka")
        .option(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.bootstrapServers)
        .option("kafka.bootstrap.servers", cfg.bootstrapServers)
        .option(ConsumerConfig.GROUP_ID_CONFIG, cfg.groupId)
        .option("spark.serializer", classOf[StringDeserializer].getCanonicalName)
        .option(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getCanonicalName)
        .option(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getCanonicalName)
        .option("startingOffsets", "earliest")
        .option("spark.sql.shuffle.partitions", 8)
        .option("subscribe", cfg.inputTopic)

      val df = dataStreamReader.load().selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      implicit val schema = df.schema

      val rowEncoder = RowEncoder(StructType(Seq(
        StructField("sepalLength", DoubleType),
        StructField("sepalWidth", DoubleType),
        StructField("petalLength", DoubleType),
        StructField("petalWidth", DoubleType)
      )))

      val irisDf = df.map(row => {
        val key = row.getString(ind("key"))
        val value = row.getString(ind("value"))
        val json: Either[circe.Error, Iris] = decode[Iris](value)
        json match {
            case Left(error) => println("Error decode message of key: " + key + ", exeption: " + error.getStackTrace.mkString)
              Row(null, null, null, null)
            case Right(iris) => Row(iris.sepalLength, iris.sepalWidth, iris.petalLength, iris.petalWidth)
          }
        })(rowEncoder).toDF()

      val irisDfWithPredict = irisDf.withColumn("predict", predictUdf(model)(col("sepalLength"), col("sepalWidth"), col("petalLength"), col("petalWidth")))
        .withColumn("json", toJsonUdf(col("sepalLength"), col("sepalWidth"), col("petalLength"), col("petalWidth"), col("predict")))
        .where(col("sepalLength").isNotNull)

      // Выводим результат
      val query = irisDfWithPredict
        .select(col("json").as("value"))
        .writeStream
        .option("checkpointLocation", cfg.checkpointLocation)
        .outputMode("append")
        .format("kafka")
        .option("kafka.bootstrap.servers", cfg.bootstrapServers)
        .option("topic", cfg.predictionTopic)
        .start()

      query.awaitTermination()
    })
  }
}
