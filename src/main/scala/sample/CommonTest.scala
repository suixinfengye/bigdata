package sample


import org.apache.spark.internal.Logging
import org.slf4j.LoggerFactory
import spark.dto.Review

import scala.collection.mutable._

/**
  * feng
  * 18-9-25
  */
object CommonTest {
  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val test: String = "fileN/media/feng/资源/bigdata/test/1291559 �$https://movie.douban.com/review/16456/:::弥新永恒不变的。\n---==---\nfileN/media/feng/资源/bigdata/test/1291560 �\u001Chttps://movie.douban.com/review/1118154/:::影片开始的的名义\n---==---"
    regx(test).foreach(r => logger.info(r.toString))
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

