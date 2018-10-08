package utils

import java.sql.Connection
import java.sql.DriverManager

import org.slf4j.LoggerFactory

/**
  * feng
  * 18-10-8
  */
object MysqlUtil {
  val logger = LoggerFactory.getLogger(this.getClass)

  def getCon: Connection = {
    try {
//      Class.forName("com.mysql.jdbc.Driver")
      Class.forName("com.mysql.cj.jdbc.Driver")
      val url = CommonUtil.getMysqlurl
      val user = CommomConfig.MYSQL_USER
      val password = CommomConfig.MYSQL_PASSWORD
      val conn = DriverManager.getConnection(url, user, password)
      logger.info(conn.getMetaData.getURL)
      conn
    } catch {
      case e: Exception =>
        logger.error(e.getMessage)
        throw e
    }
  }

  def colseConnection(con: Connection): Unit = {
    try {
      if (con != null) {
        con.close()
      }
    } catch {
      case e: Exception =>
        logger.error(e.getMessage)
    }
  }
}
