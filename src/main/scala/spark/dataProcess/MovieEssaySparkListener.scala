package spark.dataProcess

import java.util.Date

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerJobEnd}
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator
import org.apache.phoenix.spark._
import spark.dto.SteamingRecord
import utils.{MyConstant, MyDateUtil}

/**
  * 在streaming application关闭时,插入保存记录
  * feng
  * 18-10-4
  */
class MovieEssaySparkListener(spark:SparkSession,acc: LongAccumulator) extends SparkListener with Logging {

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    val date = new Date
    val dateStr = MyDateUtil.dateFormat(date)
    val recordType = MyConstant.RECORD_TYPE_MED

    logInfo("---------------onApplicationEnd-----------------")
    val dataSet = List(("45655",dateStr,acc.value,recordType,date))

//    SteamingRecord("MovieEssay" + dateStr, dateStr, acc.value, recordType, t._1)
//    val dataSet = List(Aa(1), Aa(2))
    val a= spark.sparkContext
      .parallelize(dataSet)
    a.saveToPhoenix(
      "STEAMING_RECORD",
      Seq("ID", "TIME", "RECORDCOUNT","RECORDTYPE","CREATED_TIME"),
      zkUrl = Some("localhost:2181")
    )

//    save to mysql
  }
}
