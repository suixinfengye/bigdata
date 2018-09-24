package spark.dataProcess

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferBrokers
import utils.LoggerConfig

/**
  * 整合flume kafka hbase spark-streaming 将文章内容解析后储存于hbase
  * feng
  * 18-9-24
  */
object StoreMovieEssay {
  def main(args: Array[String]): Unit = {
    LoggerConfig.setStreamingLogLevels()
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("StoreMovieEssay")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .getOrCreate()

    val kafkaParams = Map[String, Object](
      //      "bootstrap.servers" -> "localhost:9092",
      "bootstrap.servers" -> "192.168.0.101:9092,192.168.0.107:9092,192.168.0.108:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "StoreMovieEssayGroup",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )
    val topics = Array("test-flume-topic")
    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))
    ssc.checkpoint("/home/feng/software/code/bigdata/spark-warehouse")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferBrokers,
      Subscribe[String, String](topics, kafkaParams)
    )

    processData(spark,stream)

    ssc.start()
    ssc.awaitTermination()
  }

  def processData(spark:SparkSession,stream:InputDStream[ConsumerRecord[String, String]]): Unit = {
    stream.map(mapFunc = record => (record.key, record.value)).foreachRDD(r => r.collect().foreach(t => print("----------------------------------------" + t)))
  }
}
