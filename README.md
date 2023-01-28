# Построение и применение модели

## Запуск окружения

### Запуск zeppelin для разработки модели 
docker run -u $(id -u) -p 8080:8080 -p 4040:4040 --rm -v /opt/tools/spark-3.1.2-bin-hadoop3.2:/opt/spark -e SPARK_HOME=/opt/spark --name zeppelin apache/zeppelin:0.10.0

### Запуск kafka для разработки сервиса стриминга
cd $HOME/projects/otus/kafka_scala_example
docker-compose up

### Создание топиков для стриминга
bin/kafka-topics.sh --bootstrap-server localhost:29092 --create --topic input --partitions 3 --replication-factor 1
bin/kafka-topics.sh --bootstrap-server localhost:29092 --create --topic prediction --partitions 3 --replication-factor 1

### Запуск сервиса стриминга для определения Ирисов
Запустить com.github.oycii.ml.streaming.PredictStreamingMain 
с параметрами:
-m model -b localhost:29092 -g group-predict-23 -i input -o prediction -c $HOME/tmp 

### Загрузка данных во входящий топик с данными
Запустить com.github.oycii.kafka.producer.ProducerCVS

