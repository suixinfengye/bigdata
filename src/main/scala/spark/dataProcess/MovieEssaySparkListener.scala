package spark.dataProcess


import java.sql.{PreparedStatement, SQLException, Timestamp}
import java.util.Date

import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator
import org.slf4j.LoggerFactory
import spark.dto.SteamingRecord
import utils.{MyConstant, MyDateUtil, MysqlUtil}


/**
  * 在streaming application关闭时,插入保存记录
  * feng
  * 18-10-4
  */
class MovieEssaySparkListener(spark: SparkSession, acc: LongAccumulator, startTime: Timestamp) extends SparkListener {

  val logger = LoggerFactory.getLogger(this.getClass)

  /**
    * 在streaming application关闭时,插入保存记录
    * 统计在整个streaming 运行期间读取的记录数
    * 使用次数太少,不用数据库连接池
    *
    * @param applicationEnd
    */
  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    logger.info("---------------onApplicationEnd-----------------")
    val date = new Date
    val dateStr = MyDateUtil.dateFormat(date)
    val curretTime = new Timestamp(date.getTime)
    val recordType = MyConstant.RECORD_TYPE_MED

    val record = SteamingRecord(dateStr, startTime, curretTime, acc.value, recordType, "EssayMED",
      curretTime, null)
    val prepareStatementStr = "INSERT INTO steaming_record(id,startTime, endTime," +
      "recordCount,recordType, batchRecordId,created_time) VALUES(?,?,?,?,?,?,?)"
    logger.info("prepareStatement : "+prepareStatementStr)
    logger.info(record.toString)

    val con = MysqlUtil.getCon
    try {
      val pstmt: PreparedStatement = con.prepareStatement(prepareStatementStr)
      pstmt.setString(1,record.id)
      pstmt.setTimestamp(2, record.startTime)
      pstmt.setTimestamp(3, record.endTime)
      pstmt.setInt(4, record.recordCount.toInt)
      pstmt.setString(5, record.recordType)
      pstmt.setString(6, record.batchRecordId)
      pstmt.setTimestamp(7, record.createdTime)
      pstmt.executeUpdate
    } catch {
      case e: SQLException =>
        logger.error(e.getMessage)
    } finally {
      MysqlUtil.colseConnection(con)
    }

  }
}
