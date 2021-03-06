package sample


import java.nio.charset.Charset
import java.sql
import java.sql.Timestamp
import java.util.{Calendar, Date}

import org.apache.commons.lang.time.FastDateFormat
import org.apache.commons.lang3.time.DateUtils
import org.slf4j.LoggerFactory
import spark.dto.Review
import utils.{CommomConfig, CommonUtil, MyDateUtil}

import scala.collection.mutable._

/**
  * feng
  */
object CommonTest {
  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    //    val test: String = "fileN/media/feng/资源/bigdata/test/1291559 �$https://movie.douban.com/review/16456/:::弥新永恒不变的。\n---==---\nfileN/media/feng/资源/bigdata/test/1291560 �\u001Chttps://movie.douban.com/review/1118154/:::影片开始的的名义\n---==---"
    //    regx(test).foreach(r => logger.info(r.toString))
    //    reverse("21565")
    //    testConfig
    //    testDateFormat
//    getCharset

    val s = "10001418-5584520"
    val keys = s.split("-")
    print(keys(0)+" "+keys(1))
//    val list = "dfdf".split("---==---")
//    list.foreach { item =>
//      println("___")
//    }
//    whereClass
//
//    val buf = scala.collection.mutable.ListBuffer.empty[Int]
//    for (i <- 0 to 20000000) {
//      buf += i
//    }
  }

  import org.junit.Test
  import javax.servlet.http.HttpServletRequest

  @Test def whereClass(): Unit = {
    System.out.println(classOf[HttpServletRequest].getProtectionDomain.getCodeSource.toString)
  }
//  def testNull: Unit ={
//    reco
//  }

  def getCharset={
    val c:Charset =Charset.defaultCharset()
    logger.info(c.name())
  }
  //1539469197000
  def testTimeStamp = {
    //    logger.info(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format("1539469197000"))
    //    logger.info(new Date(1539469197000l).toString)
    val date = new Date(13396 * 1000 * 1000 * 1000)
    //    date.setDate(13396)

    val calendar: Calendar = Calendar.getInstance
    calendar.after()
    //    val d = DateUtils.addDays(new Date(0),13396)

    val d: Date = DateUtils.addDays(new Date(0), 13396)
    val sss = new sql.Date(d.getTime)
    //    val d: Date = DateUtils.addDays(new Date(0), MyDateUtil.getDate(13396))
    //    new sql.Date(d.getTime)
    val s = FastDateFormat.getInstance("yyyy-MM-dd").format(sss)
    logger.info(s)
  }

  def testDateFormat = {
    val date = new Date
    val dateStr = MyDateUtil.dateFormat(date)
    val curretTime = new Timestamp(date.getTime)
    logger.info("curretTime:" + curretTime)
    logger.info("dateStr:" + dateStr)

    val fdf: FastDateFormat = FastDateFormat.getInstance("MM-dd")
    logger.info(fdf.format(new Date()))

  }


  def testConfig(): Unit = {
    logger.info(CommonUtil.getKafkaServers)
    CommomConfig.isTest = false
    logger.info(CommonUtil.getKafkaServers)
  }


  def reverse(movieid: String) = {
    logger.info(movieid)
    logger.info(movieid.reverse)
  }

  /**
    * 解析生成影评对象集合
    *
    * @param stringContent
    * @return
    */
  def regx(stringContent: String): List[Review] = {

    val list = stringContent.split("---==---")
    val pattern = "([0-9]{5,})".r
    val fileNamePattern = "/bigdata/test/"
    val reviewPattern = "https://movie.douban.com/review/"
    val contentPattern = ":::"
    val reviewList: ListBuffer[Review] = ListBuffer[Review]()
    list.foreach { item =>
      if (item.length > 90) {
        val fileNameIndex: Int = item.indexOf(fileNamePattern)
        val reviewIndex: Int = item.indexOf(reviewPattern)
        val contentIndex: Int = item.indexOf(contentPattern)
        val movieid = pattern findFirstIn item.substring(fileNameIndex + 14, reviewIndex)
        val reviewid = item.substring(reviewIndex + 32, contentIndex - 1)
        val content = item.substring(contentIndex + 3).trim
        reviewList.append(Review(movieid.get, reviewid, content))
      } else {
        logger.info("empty review:" + item)
      }
    }
    reviewList.toList
  }
}

