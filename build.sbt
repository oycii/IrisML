name := "IrisML"
version := "1.0"
scalaVersion := "2.12.10"

lazy val sparkVersion = "3.2.2"
lazy val kafkaVersion = "3.3.1"
val circeVersion = "0.14.3"
val json4sVersion = "3.6.6"
val scoptVersion = "4.1.0"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-mllib_2.12"                % sparkVersion,
  "org.apache.spark" % "spark-streaming_2.12"            % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" % "spark-streaming-kafka-0-10_2.12" % sparkVersion,
  "org.apache.spark" % "spark-sql-kafka-0-10_2.12" % sparkVersion,
  "org.apache.kafka" % "kafka-clients"                   % kafkaVersion,
  "com.github.tototoshi" %% "scala-csv" % "1.3.10",
  "org.apache.commons" % "commons-csv" % "1.9.0",
  "io.circe" %% "circe-core" % circeVersion,
  "io.circe" %% "circe-generic" % circeVersion,
  "io.circe" %% "circe-parser" % circeVersion,
  "org.json4s" %% "json4s-jackson" % json4sVersion,
  "org.json4s" %% "json4s-ast" % json4sVersion,
  "org.json4s" %% "json4s-scalap" % json4sVersion,
  "org.json4s" %% "json4s-core" % json4sVersion,
  "com.github.scopt" %% "scopt" % "4.1.0",
  "io.scalaland" %% "chimney" % "0.6.2",
  "ch.qos.logback" % "logback-classic" % "1.3.2",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
  "com.typesafe" % "config" % "1.4.2",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.10.0"
)

