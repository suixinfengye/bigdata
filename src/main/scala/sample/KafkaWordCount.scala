
package sample

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferBrokers
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
//ps -ef | grep spark |  grep KafkaWordCount | awk '{print $2}'   | xargs kill  -SIGTERM

//./bin/spark-submit \
//--class sample.KafkaWordCount \
//--master spark://feng:7077 \
//--executor-memory 1G \
//--total-executor-cores 2 \
///home/feng/software/code/bigdata/out/artifacts/bigdata/bigdata.jar
object KafkaWordCount {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
//      .master("local")
      .appName("KafkaWordCount")
      .config("spark.streaming.stopGracefullyOnShutdown","true")
      .getOrCreate()

    val kafkaParams = Map[String, Object](
//      "bootstrap.servers" -> "localhost:9092",
      "bootstrap.servers" -> "192.168.0.101:9092,192.168.0.107:9092,192.168.0.108:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "KafkaWordCountgroup",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )
//    val topics = Array("KafkaWordCountTopicTest")
    val topics = Array("test1")
    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))
//    ssc.checkpoint("hdfs://localhost:9000//spark//checkpoint")
    ssc.checkpoint("hdfs://spark1:9000//spark//checkpoint")
    //    ssc.checkpoint("/home/feng/software/code/bigdata/spark-warehouse")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferBrokers,
      Subscribe[String, String](topics, kafkaParams)
    )


    stream.map(mapFunc = record => (record.key, record.value)).foreachRDD(r=>r.collect().foreach(t=>print("----------------------------------------"+t)))

    ssc.start()
    ssc.awaitTermination()
  }
}

