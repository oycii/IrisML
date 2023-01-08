package com.github.oycii.kafka.producer

import com.github.oycii.entity.Iris
import java.io.FileReader
import org.apache.commons.csv.{CSVFormat, CSVRecord}
import io.circe.syntax._

import scala.language.implicitConversions
import io.circe.generic.auto._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization._
import java.util.Properties

object ProducerCVS {
  val servers = "localhost:29092"
  val topic = "input"
  val group = "g3"

  def getIris(rec: CSVRecord): Iris = {
    Iris(rec.get("sepal_length").toDouble, rec.get("sepal_width").toDouble, rec.get("petal_length").toDouble, rec.get("petal_width").toDouble, rec.get("species"))
  }

  def send: Unit = {
    val in = new FileReader("data/IRIS.csv")
    val records = CSVFormat.DEFAULT.withFirstRecordAsHeader.parse(in)

    val props = new Properties()
    props.put("bootstrap.servers", servers)
    val producer: KafkaProducer[String, String] = new KafkaProducer(props, new StringSerializer, new StringSerializer)
    try {
      val it = records.iterator
      while (it.hasNext) {
        val list = it.next()
        println("list: " + list)
        println("size: " + list.size)
        val iris = getIris(list)
        val json = iris.asJson
        println(json)
        producer.send(new ProducerRecord(topic, iris.hashCode.toString, json.toString()))
      }
      producer.flush()
    } finally {
      producer.close()
      in.close()
    }
  }

  def main(args: Array[String]): Unit = {
    send
  }

}
