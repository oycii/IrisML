package com.github.oycii.utils

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.util.SizeEstimator
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

import java.lang.management.ManagementFactory
import java.util.Locale

object SparkUtils {
  private val log: Logger = LoggerFactory.getLogger(SparkUtils.getClass)

  object OS extends Enumeration {
    val WINDOWS: String = "win"
    val LINUX: String = "linux"
    val MAC: String = "mac"
  }

  val isIDE: Boolean = {
    val args = ManagementFactory.getRuntimeMXBean.getInputArguments.toString.toLowerCase
    args.contains("intellij") || args.contains("idea")
  }

  val isLinux: Boolean = {
    val osName = System.getProperty("os.name", "generic").toLowerCase(Locale.ENGLISH)
    osName.contains("nix") || osName.contains("nux") || osName.contains("aix")
  }

  def getSpark(appName: String): SparkSession = {
    val conf = new SparkConf().setAppName(appName)
    var spark: SparkSession = null
    if (isIDE && !isLinux) {
      log.info("getSpark: isIDE && !isLinux")
      System.setProperty("hadoop.home.dir", "C:\\hadoop")
      conf.set("getSpark: fs.defaultFS", "hdfs://localhost:9000")
      conf.set("spark.driver.bindAddress", "127.0.0.1")
      conf.set("spark.streaming.kafka.maxRatePerPartition", "10")
      conf.setMaster("local[*]")
      spark = SparkSession.builder().config(conf).master("local").getOrCreate()
    } else if (isIDE && isLinux) {
      System.setProperty("hadoop.home.dir", "/home/sanya/tmp")
      log.info("getSpark: isIDE && isLinux")
      conf.set("fs.defaultFS", "file://home/sanya/tmp")
      conf.set("spark.streaming.kafka.maxRatePerPartition", "10")

      conf.setMaster("local[*]")
      //val streamingContext = new StreamingContext(conf, Seconds(1))
      //val sparkContext = streamingContext.sparkContext

      //spark = SparkSessionSingleton.getInstance(conf)
      spark =  SparkSession.builder().config(conf).master("local").getOrCreate()
    } else if (System.getenv("DEFAULT_FS") != null) {
      log.info("getSpark: DEFAULT_FS")
      log.info("DEFAULT_FS: " + System.getenv("DEFAULT_FS"))
      conf.set("fs.defaultFS", System.getenv("DEFAULT_FS"))
      conf.setMaster("local[*]")
      spark = SparkSession.builder().config(conf).master("local").getOrCreate()
    } else {
      log.info("getSpark")
      spark = SparkSession.builder().config(conf).getOrCreate()
    }
    spark
  }

  def mkDir(path: String, sc: SparkContext): Unit = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    fs.mkdirs(new Path(path))
  }

  def delete(path: String, sc: SparkContext): Unit = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    fs.deleteOnExit(new Path(path))
  }

  def pathExists(path: String, sc: SparkContext): Boolean = {
    val conf = sc.hadoopConfiguration
    val fs = FileSystem.get(conf)
    fs.exists(new Path(path))
  }

  def dirEmpty(path: String, sc: SparkContext): Boolean = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val files = fs.listFiles(new Path(path), true)
    !files.hasNext
  }

  def getFullPath(path: String, sc: SparkContext): String = {
    val conf = sc.hadoopConfiguration
    val fs = FileSystem.get(conf)
    fs.getFileStatus(new Path(path)).getPath.toString
  }

  def getAllFiles(path: String, sc: SparkContext): Seq[String] = {
    val conf = sc.hadoopConfiguration
    val fs = FileSystem.get(conf)
    val files = fs.listStatus(new Path(path))
    files.map(_.getPath.toString)
  }

  def getConf(path: String, sc: SparkContext): Config = {
    if (path != null) {
      val conf = sc.wholeTextFiles(path).collect()
      ConfigFactory.parseString(conf(0)._2.toString)
    } else {
      ConfigFactory.load("application.conf")
    }
  }

  def getSchema(path: String, sc: SparkContext): Option[StructType] = {
    try {
      val schemaJson = sc.wholeTextFiles(path).collect()
      Some(DataType.fromJson(schemaJson(0)._2.toString).asInstanceOf[StructType])
    } catch {
      case e: Exception => e.printStackTrace()
        log.error("table " + path + " is reading without particular schema !")
        None
    }
  }


  def withSparkSession(appName: String, op: SparkSession => Unit): Unit = {
    val spark = SparkUtils.getSpark(appName)
    try {
      op(spark)
    } finally {
      if (spark != null) {
        spark.close()
      }
      log.info("Close spark session: " + appName)
    }
  }

  /** Преобразование из df в rdd и обратно в df, так как Java функция сгенерированная catalyst получилась более 64Кb,
    * такой performance tuning по рекомедации разаработчиков Spark */
  def dfToRddToDF(df: DataFrame): DataFrame = {
    val sparkSession = SparkUtils.currentSparkSession(df.sqlContext.sparkContext.getConf)
    val schema = df.schema
    val rows: RDD[Row] = df.rdd
    sparkSession.createDataFrame(rows, schema)
  }

  def currentSparkSession(conf: SparkConf): SparkSession = {
    SparkSession.builder().config(conf).getOrCreate()
  }
}
