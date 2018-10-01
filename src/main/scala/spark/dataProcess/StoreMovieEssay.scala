package spark.dataProcess


import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferBrokers
import spark.dto.Review
import utils.{CommomConfig, CommonUtil}

import scala.collection.mutable.ListBuffer

/**
  * 整合flume kafka hbase spark-streaming 将文章内容解析后储存于hbase
  * hbase key为 reverse(movieid)-reviewid
  *
  * ./kafka-topics.sh --create --zookeeper 192.168.0.101:2181,192.168.0.107:2181,192.168.0.108:2181 --replication-factor 2 --partitions 2 --topic movie-essay-topic
  *
  * feng
  * 18-9-24
  */
object StoreMovieEssay extends Logging {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("StoreMovieEssay")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.streaming.backpressure.enabled", "true")
      .config("spark.defalut.parallelism", "6")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()


    // config kafka
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> CommonUtil.getKafkaServers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "StoreMovieEssayGroup",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )
    val topics = Array(getTopic)

    //config spark Streaming
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    ssc.checkpoint(CommonUtil.getCheckpointDir)
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferBrokers,
      Subscribe[String, String](topics, kafkaParams)
    )

    processData(spark, stream)

    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 把记录存入hbase
    *
    * @param spark
    * @param stream
    */
  def processData(spark: SparkSession, stream: InputDStream[ConsumerRecord[String, String]]): Unit = {
    stream.map(mapFunc = record => regx(record.value)).filter(list => list.nonEmpty).foreachRDD {
      r =>  //List[List[Review]]
        if (!r.isEmpty()) {
          val reviewList = r.filter(list => list.nonEmpty).reduce(_ ++ _)
          logInfo("save reviewList:"+reviewList.size)
          val jobConf = CommonUtil.getWriteHbaseConfig(spark,"test")
          spark.sparkContext.parallelize(reviewList).map(converToPut).saveAsHadoopDataset(jobConf)
        }
        logInfo("rdd[list[list[Review]]] is empty"+r.id)
    }
  }

  /**
    * 解析生成影评对象集合
    *
    * @param stringContent
    * @return
    */
  def regx(stringContent: String): List[Review] = {
    val list = stringContent.split("---==---")
    val pattern = "([0-9]{5,})".r
    val fileNamePattern = "/bigdata/test/"
    val reviewPattern = "https://movie.douban.com/review/"
    val contentPattern = ":::"
    val reviewList: ListBuffer[Review] = ListBuffer[Review]()
    list.foreach { item =>
      if (item.length > 90) { //过滤空对象
        val fileNameIndex: Int = item.indexOf(fileNamePattern)
        val reviewIndex: Int = item.indexOf(reviewPattern)
        val contentIndex: Int = item.indexOf(contentPattern)
        val movieid = pattern findFirstIn item.substring(fileNameIndex + 14, reviewIndex)
        val reviewid = item.substring(reviewIndex + 32, contentIndex - 1)
        val content = item.substring(contentIndex + 3).trim
        reviewList.append(Review(movieid.get, reviewid, content))
      } else {
        logInfo("empty review:" + item)
      }
    }
    reviewList.toList
  }

  /**
    * 转换成 put
    *
    * @param c
    * @return
    */
  def converToPut(c: Review) = {
    // rowkey设计 均衡分布记录
    val put = new Put(Bytes.toBytes(c.movieid.reverse + "-" + c.reviewid))
    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("cn"), Bytes.toBytes(c.content))
    (new ImmutableBytesWritable, put)
  }

  def getTopic:String={
    val topics = Array("test-flume-topic")
    if (CommomConfig.isTest){
      "test-flume-topic"
    }else{
      "movie-essay-topic"
    }
  }
}
