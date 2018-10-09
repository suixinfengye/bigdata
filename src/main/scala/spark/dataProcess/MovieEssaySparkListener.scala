package spark.dataProcess


import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerJobEnd}
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator
import org.apache.phoenix.spark._
import spark.dto.SteamingRecord
import utils.{MyConstant, MyDateUtil, MysqlUtil}
import java.sql.{PreparedStatement, SQLException, Timestamp}
import java.util.Date

import org.slf4j.LoggerFactory


/**
  * 在streaming application关闭时,插入保存记录
  * feng
  * 18-10-4
  */
class MovieEssaySparkListener(spark: SparkSession, acc: LongAccumulator) extends SparkListener with Logging {

  val logger = LoggerFactory.getLogger(this.getClass)

  /**
    * 在streaming application关闭时,插入保存记录
    * 统计在整个streaming 运行期间读取的记录数
    *
    * @param applicationEnd
    */
  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    val date = new Date
    val dateStr = MyDateUtil.dateFormat(date)
    val recordType = MyConstant.RECORD_TYPE_MED

    logInfo("---------------onApplicationEnd-----------------")

    val record = SteamingRecord(null, dateStr, acc.value, recordType, "Essay_MED", new Timestamp(date.getTime
    ()), null)

    val con = MysqlUtil.getCon
    //TODO 存多一个起始时间吧
    try {
      val pstmt: PreparedStatement = con.prepareStatement("INSERT INTO steaming_record(time, recordCount," +
        " recordType, batchRecordId,created_time) VALUES(?,?,?,?,?)")
      pstmt.setString(1, record.time)
      pstmt.setInt(2, 10)
      pstmt.setString(3, record.recordType)
      pstmt.setString(4, record.batchRecordId)
      pstmt.setTimestamp(5, record.createdTime)
      pstmt.executeUpdate
    } catch {
      case e: SQLException =>
        logger.error(e.getMessage)
    } finally {
      MysqlUtil.colseConnection(con)
    }
  }

}
