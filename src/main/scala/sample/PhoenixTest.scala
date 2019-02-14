package sample

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, SQLException, Statement, Timestamp}
import java.util.{Date, UUID}

import org.slf4j.LoggerFactory
import utils.CommonUtil

/**
  * feng
  */
object PhoenixTest {

  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    val con: Connection = CommonUtil.getPhoenixConnection
    testSTEAMING_RECORD(con)
  }

  def testSTEAMING_RECORD(con: Connection): Unit = {
    val sql = "upsert into STEAMING_RECORD(ID,STARTTIME,ENDTIME,RECORDCOUNT,RECORDTYPE,BATCHRECORDID,CREATEDTIME) " +
      "values(?,CONVERT_TZ(?, 'UTC', 'Asia/Shanghai'),CONVERT_TZ(?, 'UTC', 'Asia/Shanghai'),?,?,?,CONVERT_TZ(CURRENT_DATE(), 'UTC', 'Asia/Shanghai'))"
    val pstmt: PreparedStatement = con.prepareStatement(sql)

    for (a <- 15552 until (15553)) {
      pstmt.setString(1, a + "")
      pstmt.setTimestamp(2, new Timestamp(new Date().getTime))
      pstmt.setTimestamp(3, new Timestamp(new Date().getTime))
      pstmt.setInt(4, 15)
      pstmt.setString(5, null)
      pstmt.setString(6, "BATCHRECORDID")
      pstmt.addBatch()
    }
    executeAndCommit(con, pstmt)
  }

  def testTBL(con: Connection): Unit = {
    val sql = "upsert into tbl(id,title,author) VALUES (?,?,?)"
    val pstmt: PreparedStatement = con.prepareStatement(sql)
    pstmt.setInt(1, 15)
    pstmt.setString(2, "title")
    pstmt.setString(3, "author")
    pstmt.addBatch()
    executeAndCommit(con, pstmt)
  }

  def executeAndCommit(con: Connection, ps: PreparedStatement) = {
    ps.executeBatch()
    con.commit()
    con.close()
  }

}
