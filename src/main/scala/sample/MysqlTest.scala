package sample

import java.sql.{PreparedStatement, SQLException, Time, Timestamp}
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
    mysql
  }

  def mysql = {
    val date = new Date
    val dateStr = MyDateUtil.dateFormat(date)
    val recordType = MyConstant.RECORD_TYPE_MED
    val record = SteamingRecord("MovieEssay" + dateStr, dateStr, 15, recordType, "BATCHid",new Timestamp(date.getTime
    ()),null)
    val con = MysqlUtil.getCon

    try {
      val pstmt: PreparedStatement = con.prepareStatement("INSERT INTO steaming_record(time, recordCount," +
        " recordType, batchRecordId,created_time) VALUES(?,?,?,?,?)")

      pstmt.setString(1, record.time)
      pstmt.setInt(2, 10)
      pstmt.setString(3, record.recordType)
      pstmt.setString(4, record.batchRecordId)
      pstmt.setTimestamp(5,record.createdTime)
      pstmt.executeUpdate
    } catch {
      case e: SQLException =>
        logger.error(e.getMessage)
    } finally {
      MysqlUtil.colseConnection(con)
    }

  }
}
