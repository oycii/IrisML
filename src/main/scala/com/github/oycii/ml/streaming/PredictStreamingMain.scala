package com.github.oycii.ml.streaming

import com.github.oycii.entity.Iris
import com.github.oycii.ml.config.{AppConfig, AppConfigParser}
import com.github.oycii.model.{Consts, ModelBuilderMain}
import com.github.oycii.utils.{KafkaSink, SparkSessionSingleton}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import io.circe.generic.auto._
import io.circe.parser.decode

import java.util.Properties
import com.github.oycii.utils.SparkUtils
import io.circe
import io.circe.syntax.EncoderOps
import org.apache.spark.mllib.tree.model.RandomForestModel

object PredictStreamingMain {

  def main(args: Array[String]): Unit = {

    println(Consts.irisNames(0))
    val argsParser = AppConfigParser.parser()
    val appConfig = argsParser.parse(args, AppConfig())

    val cfg: AppConfig = appConfig match {
      case Some(value) => value
      case None =>
        throw new Exception("Usage: MLStreaming <path-to-model> <bootstrap-servers> <groupId> <input-topic> <prediction-topic>")
    }

    // Создаём Streaming Context и получаем Spark Context
    //val sparkConf = new SparkConf().setAppName("MLStreaming")
    SparkUtils.withSparkSession("MLStreaming", spark => {
      val streamingContext = new StreamingContext(spark.sparkContext, Seconds(1))
      val sparkContext = streamingContext.sparkContext
      sparkContext.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive","true")
      sparkContext.hadoopConfiguration.set("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive","true")

      val model = spark.sparkContext.objectFile[RandomForestModel](cfg.pathToModel).first()

      // Создаём свойства Producer'а для вывода в выходную тему Kafka (тема с расчётом)
      val props: Properties = new Properties()
      props.put("bootstrap.servers", cfg.bootstrapServers)

      // Создаём Kafka Sink (Producer)
      val kafkaSink = sparkContext.broadcast(KafkaSink(props))

      // Параметры подключения к Kafka для чтения
      val kafkaParams = Map[String, Object](
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> cfg.bootstrapServers,
        ConsumerConfig.GROUP_ID_CONFIG -> cfg.groupId,
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
      )

      // Подписываемся на входную тему Kafka (тема с данными)
      val inputTopicSet = Set(cfg.inputTopic)
      val messages = KafkaUtils.createDirectStream[String, String](
        streamingContext,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](inputTopicSet, kafkaParams)
      )

      // Обрабатываем каждый входной набор
      messages.foreachRDD { rdd =>
        // Get the singleton instance of SparkSession
        //val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
        println("rdd count: " + rdd.count())

        rdd.foreach(rec => {
          println("message: " + rec.value().mkString)
          val json: Either[circe.Error, Iris] = decode[Iris](rec.value().mkString)
          json match {
            case Left(error) => println("Error decode message of key: " + rec.key + ", exeption: " + error.getStackTrace.mkString)
            case Right(iris) =>
              val vector = org.apache.spark.mllib.linalg.Vectors.dense(iris.sepalLength, iris.sepalWidth, iris.petalLength, iris.petalWidth)
              val typeIris = model.predict(vector)
              println("iris: " + iris + ", type iris: " + typeIris)
              val predictNameIris = Consts.irisNames(typeIris.toInt)
              println("nameIris: " + predictNameIris)
              val json = iris.copy(species = predictNameIris).asJson.toString()
              println("predict json: " + json + ", expected: " + iris.species)
              kafkaSink.value.send(cfg.predictionTopic, iris.copy(species = predictNameIris).asJson.toString())
          }
        })

        kafkaSink.value.producer.flush()
      }
      streamingContext.start()
      streamingContext.awaitTermination()
    })


  }
}
