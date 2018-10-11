package spark.dataProcess

import java.sql.{Connection, PreparedStatement}

import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferBrokers
import sample.Tdl
import spark.dataProcess.StoreMovieEssay.regx
import utils.{CommomConfig, CommonUtil}

import scala.collection.mutable.ArrayBuffer

/**
  * feng
  * 18-10-9
  */
object ProcessMysqlData extends Logging {
  val batchInterval: Int = 3

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("ProcessMysqlData")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.streaming.backpressure.enabled", "true")
      .config("spark.streaming.blockInterval", "2s  ")
      .config("spark.defalut.parallelism", "6")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    //设置当前为测试环境
    CommonUtil.setTestEvn

    // config kafka
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> CommonUtil.getKafkaServers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "ProcessMysqlDataGroup",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )
    val topics: Array[String] = getMysqlTopic
    //config spark Streaming
    val ssc = new StreamingContext(spark.sparkContext, Seconds(batchInterval))
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
    * 把记录存入Phoenix
    *
    * @param spark
    * @param stream
    */
  def processData(spark: SparkSession, stream: InputDStream[ConsumerRecord[String, String]]): Unit = {
    //按表名分组(tableame,Array(tableinfo))
    val batchTableRecordDS = stream.filter(r => r.value != null).map(record => convertTableJsonStr(record.value))
      .reduceByKey(_ ++ _)

    batchTableRecordDS.foreachRDD {
        rdd: RDD[(String, ArrayBuffer[String])] =>
          if (!rdd.isEmpty()) {
            rdd.foreachPartition {
              part =>
                //get Connection in each partition
                val con: Connection = CommonUtil.getPhoenixConnection
                part.foreach {
                  info =>
                    // 对每个记录
                    info._1 match {

                      case "tbl" => {
                        val list: ArrayBuffer[Tdl] = info._2.map(t => JSON.parseObject(t, classOf[Tdl]))
                        logInfo("---------tbl-----not empty")
                      }

                      case "steaming_record" => {
                        val list: ArrayBuffer[Tdl] = info._2.map(t => JSON.parseObject(t, classOf[Tdl]))
                        var pstmt: PreparedStatement = con.prepareStatement("upsert into STEAMING_RECORD(ID,RECORDCOUNT) values (?,?)")
                        for (a <- 15552 until (15556)) {
                          pstmt.setString(1, a + "")
                          pstmt.setInt(2, a)
                          pstmt.addBatch()
                        }
                        pstmt.executeBatch()
                        con.commit()
                        con.close()
                      }

                      case _ => logInfo("---------????-----not empty" )
                    }
                }
            }
          } else {
            logInfo("--------ProcessMysqlData empty--------")
          }

          rdd.count()

      }

  }

  def convertTableJsonStr(jsonstr: String): (String, ArrayBuffer[String]) = {
    val jsonObject = JSON.parseObject(jsonstr);
    val data = jsonObject.getString("payload");
    val schema = JSON.parseObject(jsonObject.getString("schema"));
    val topicName = schema.getString("name").split("\\.")
    val tableName = topicName(topicName.size - 2)
    (tableName, ArrayBuffer(data))
  }

  def getMysqlTopic: Array[String] = {
    var topics = Array("mysqlfullfillment.test.steaming_record", "mysqlfullfillment.test.tbl")
    if (CommomConfig.isTest) {
      topics = Array("mysqlfullfillment.test.steaming_record", "mysqlfullfillment.test.tbl")
    }
    logInfo("topic:" + topics)
    topics
  }
}
