package spark.dataProcess

import java.sql.{Connection, PreparedStatement, SQLException, Timestamp}
import java.util.Date

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
import spark.dto.{Doulist, SteamingRecord}
import utils.{CommomConfig, CommonUtil, MysqlUtil}

import scala.collection.mutable.ArrayBuffer

/**
  * feng
  * 18-10-9
  */
object ProcessMysqlData extends Logging {
  val batchInterval: Int = 2

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
          // 按partition遍历
          rdd.foreachPartition {
            part =>
              //get Connection in each partition
              val con: Connection = CommonUtil.getPhoenixConnection
              part.foreach {
                info =>
                  // 对每个记录
                  info._1 match {
                    case "tbl" => saveTbl(info, con)
                    case "steaming_record" => saveSteamingRecord(info, con)
                    case "doulist" => {
                      val list: ArrayBuffer[Doulist] = info._2.map(t => JSON.parseObject(t, classOf[Doulist]))
                      logInfo("insert Doulist:" + list.size)

                    }
                    case _ => logInfo("---------not match-----")
                  }
              }
              MysqlUtil.colseConnection(con)
          }
        } else {
          logInfo("--------ProcessMysqlData empty--------")
        }
        //强制runjob
        rdd.count()
    }

  }

  def saveTbl(info: (String, ArrayBuffer[String]), con: Connection): Unit = {
    val list: ArrayBuffer[Tdl] = info._2.map(t => JSON.parseObject(t, classOf[Tdl]))
    val sql = "upsert into tbl(id,title,author) VALUES (?,?,?)"
    logInfo(sql)
    logInfo(list(0).toString)
    logInfo("insert Tdl size:" + list.size)
    val pstmt: PreparedStatement = con.prepareStatement(sql)
    list.foreach {
      r =>
        pstmt.setInt(1, r.id)
        pstmt.setString(2, r.title)
        pstmt.setString(3, r.author)
        pstmt.addBatch()
    }
    executeAndCommit(pstmt, con)
  }

  def saveSteamingRecord(info: (String, ArrayBuffer[String]), con: Connection): Unit = {
    logInfo("ArrayBuffer[String]:"+info._2(0).toString)
    val list: ArrayBuffer[SteamingRecord] = info._2.map(t => JSON.parseObject(t, classOf[SteamingRecord]))
    val executeSql = "upsert into STEAMING_RECORD(ID,STARTTIME,ENDTIME,RECORDCOUNT,RECORDTYPE,BATCHRECORDID,CREATEDTIME) " +
      "values(?,CONVERT_TZ(?, 'UTC', 'Asia/Shanghai'),CONVERT_TZ(?, 'UTC', 'Asia/Shanghai'),?,?,?,CONVERT_TZ" +
      "(CURRENT_DATE(), 'UTC', 'Asia/Shanghai'))"
    var pstmt: PreparedStatement = con.prepareStatement(executeSql)
    logInfo(executeSql)
    logInfo(list(0).toString)
    logInfo("insert steaming_record size:" + list.size)
    list.foreach {
      r =>
        pstmt.setString(1, r.id)
        pstmt.setTimestamp(2, r.startTime)
        pstmt.setTimestamp(3, r.endTime)
        pstmt.setLong(4, r.recordCount)
        pstmt.setString(5, r.recordType)
        pstmt.setString(6, r.batchRecordId)
        pstmt.addBatch()
    }
    executeAndCommit(pstmt, con)
  }

  def executeAndCommit(ps: PreparedStatement, con: Connection) = {
    try {
      ps.executeBatch()
      con.commit()
    } catch {
      case e: SQLException =>
        logError(e.getMessage)
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
