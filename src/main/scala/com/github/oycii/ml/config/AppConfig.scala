package com.github.oycii.ml.config

case class AppConfig(
    pathToModel: String = null,
    bootstrapServers: String = null,
    groupId: String = null,
    inputTopic: String = null,
    predictionTopic: String = null
)

object AppConfigParser {
  def parser(): scopt.OptionParser[AppConfig] = {
    new scopt.OptionParser[AppConfig]("StreamingMl") {

      opt[String]('m', "path-to-model")
        .valueName("<String>")
        .action((x, c) => c.copy(pathToModel = x: String))
        .text("path-to-model - Путь к модели")

      opt[String]('b', "bootstrap-servers")
        .valueName("<String>")
        .action((x, c) => c.copy(bootstrapServers = x: String))
        .text("bootstrap-servers - Список брокеров кафки")

      opt[String]('g', "groupId")
        .valueName("<String>")
        .action((x, c) => c.copy(groupId = x: String))
        .text("groupId - Группа consumer для чтения данных из кафки")

      opt[String]('i', "input-topic")
        .valueName("<String>")
        .action((x, c) => c.copy(inputTopic = x: String))
        .text("input-topic - Входящий топик с данными")

      opt[String]('o', "prediction-topic")
        .valueName("<String>")
        .action((x, c) => c.copy(predictionTopic = x: String))
        .text("prediction-topic - Исходящий топик с результатами вычислений")

    }
  }
}
