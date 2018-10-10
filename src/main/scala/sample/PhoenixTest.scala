package sample

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, SQLException, Statement}
import java.util.UUID

import org.slf4j.LoggerFactory
import utils.CommonUtil

/**
  * feng
  * 18-10-11
  */
object PhoenixTest {

  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    val con: Connection = CommonUtil.getPhoenixConnection
    var pstmt: PreparedStatement = con.prepareStatement("upsert into STEAMING_RECORD(ID,RECORDCOUNT) values (?,?)")

    for (a <- 15552 until (15556)) {
      pstmt.setString(1,a+"")
      pstmt.setInt(2,a)
      pstmt.addBatch()
    }
    pstmt.executeBatch()
    con.commit()
    con.close()
  }


}
