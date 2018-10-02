package utils

import java.util.Date

import org.apache.commons.lang.time.FastDateFormat

/**
  * feng
  * 18-10-2
  */
object MyDateUtil {

  val fdf: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  val fdfDate: FastDateFormat = FastDateFormat.getInstance("MM-dd")

  def dateFormat(date: Date) = {
    fdf.format(date)
  }

  def simpleDateFormat(date: Date) = {
    fdfDate.format(date)
  }
}
