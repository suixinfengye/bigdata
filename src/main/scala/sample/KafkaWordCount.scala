
package sample

import com.alibaba.fastjson.JSON
import javax.servlet.http.HttpServletRequest
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferBrokers
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.kafka.connect.json.JsonDeserializer
import org.apache.spark.streaming.dstream.InputDStream
import utils.CommonUtil

import scala.collection.mutable.ArrayBuffer

//ps -ef | grep spark |  grep KafkaWordCount | awk '{print $2}'   | xargs kill  -SIGTERM

//./bin/spark-submit \
//--class sample.KafkaWordCount \
//--master spark://feng:7077 \
//--executor-memory 1G \
//--total-executor-cores 2 \
///home/feng/software/code/bigdata/out/artifacts/bigdata/bigdata.jar
object KafkaWordCount extends Logging {

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
//      .master("local")
      .appName("KafkaWordCount")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    //    simpleTestCode(spark)
    simpleClusterTest(spark)
  }

  def kafkaTest(spark: SparkSession): Unit = {
    /**
      * earliest
      * 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
      * latest
      * 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
      * none
      * topic各分区都存在已提交的offset时，从offset后开始消费；只要有一个分区不存在已提交的offset，则抛出异常
      */
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
    val topics = Array("test-flume-topic")
    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))
    //    ssc.checkpoint("hdfs://localhost:9000//spark//checkpoint")

    //        ssc.checkpoint("hdfs://spark1:9000//spark//checkpoint")
    ssc.checkpoint("/home/feng/software/code/bigdata/spark-warehouse")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferBrokers,
      Subscribe[String, String](topics, kafkaParams)
    )
    logInfo("ssssssss")

    stream.map(mapFunc = record => (record.key, record.value)).foreachRDD(r => r.collect().foreach(t => print("----------------------------------------" + t)))

    ssc.start()
    ssc.awaitTermination()
  }

  def simpleTest(spark: SparkSession): Unit = {
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[JsonDeserializer],
      "group.id" -> "KafkaWordCountgroup",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )
    //    val topics = Array("connect-test")
    val topics = Array("test-flume-topic")
    val ssc = new StreamingContext(spark.sparkContext, Seconds(2))
    //    ssc.checkpoint("hdfs://localhost:9000//spark//checkpoint")

    ssc.checkpoint("/home/feng/software/code/bigdata/spark-warehouse")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferBrokers,
      Subscribe[String, String](topics, kafkaParams)
    )

    stream.map(mapFunc = record => (record.key, record.value)).foreachRDD(
      r => r.collect().foreach(t => print("---------" + t)))

    ssc.start()
    ssc.awaitTermination()
  }

  def simpleTestCode(spark: SparkSession): Unit = {
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "KafkaWordCountgroup",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )
    val topics = Array("mysqlfullfillment.test.doulist")
    val ssc = new StreamingContext(spark.sparkContext, Seconds(2))

    ssc.checkpoint("/home/feng/software/code/bigdata/spark-warehouse")
    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferBrokers,
      Subscribe[String, String](topics, kafkaParams)
    ) //拉取kafka数据


    stream.map(mapFunc = record => (record.key, record.value)).foreachRDD(
      r => r.collect().foreach(t => print("message:" + convertTableJsonStr2(t._2))))

    ssc.start()
    ssc.awaitTermination()
  }

  def convertTableJsonStr2(jsonstr: String): (String, ArrayBuffer[String]) = {
    val jsonObject = JSON.parseObject(jsonstr);
    val data = jsonObject.getString("payload");
    val schema = JSON.parseObject(jsonObject.getString("schema"));
    val topicName = schema.getString("name").split("\\.")
    val tableName = topicName(topicName.size - 2)
    println(tableName + "data:" + data)
    (tableName, ArrayBuffer(data))
  }

  def simpleClusterTest(spark: SparkSession): Unit = {
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> CommonUtil.getKafkaServers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[JsonDeserializer],
      "group.id" -> "KafkaWordCountgroup",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )
    //    val topics = Array("connect-test")
    print("hhhhhhhhhhhhhhhhhhhh")
    logInfo("=====================:"+classOf[HttpServletRequest].getProtectionDomain.getCodeSource.toString)
    val topics = Array("mysql-clustera.test.doulist")
    val ssc = new StreamingContext(spark.sparkContext, Seconds(8))
//    ssc.checkpoint("hdfs://localhost:9000//spark//checkpoint")
    ssc.checkpoint("hdfs://spark1:9000//spark//checkpoint")
    //    ssc.checkpoint("/home/feng/software/code/bigdata/spark-warehouse")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferBrokers,
      Subscribe[String, String](topics, kafkaParams)
    )

    stream.map(mapFunc = record => (record.key, record.value)).foreachRDD(
      r => r.collect().foreach(t => print("---------" + t)))
    stream.
    stream.foreachRDD{
      t=>
        val buf = scala.collection.mutable.ListBuffer.empty[Int]
        for (i <- 0 to 20000000) {
          buf += i
        }
        t.count()
    }
    ssc.start()
    ssc.awaitTermination()
  }
}

