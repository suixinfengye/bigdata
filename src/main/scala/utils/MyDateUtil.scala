package utils

import java.sql
import java.util.Date

import org.apache.commons.lang.time.FastDateFormat
import org.apache.commons.lang3.time.DateUtils

/**
  * feng
  * 18-10-2
  */
object MyDateUtil {

  val fdf: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  val fdfMD: FastDateFormat = FastDateFormat.getInstance("MM-dd")
  val fdfYMD: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd")

  def dateFormat(date: Date) = {
    fdf.format(date)
  }

  def simpleDateFormat(date: Date) = {
    fdfMD.format(date)
  }


  /**
    * 从1970.1.1到现在经过的日期
    * @param dateTo1970
    * @return
    */
  def getDate(dateTo1970:Int):sql.Date={
    val d:Date = DateUtils.addDays(new Date(0),dateTo1970)
    new sql.Date(d.getTime)
//    val s = fdfYMD.format(d)
  }
}
