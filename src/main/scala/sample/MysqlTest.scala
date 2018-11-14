package sample

import java.sql.{PreparedStatement, SQLException, Timestamp}
import java.util.Date

import org.slf4j.LoggerFactory
import spark.dto.SteamingRecord
import utils.{MyConstant, MyDateUtil, MysqlUtil}

/**
  * feng
  * 18-10-8
  */
object MysqlTest {

  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    //设置当前为测试环境
//    CommonUtil.setTestEvn
    mysql
  }

  def mysql = {
    val date = new Date
    val dateStr = MyDateUtil.dateFormat(date)
    val curretTime = new Timestamp(date.getTime)
    val recordType = MyConstant.RECORD_TYPE_MED

    val record = SteamingRecord(dateStr, curretTime, curretTime, 15, recordType, "Essay_MED",
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
