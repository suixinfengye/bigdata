package spark.dataProcess

import java.sql.{Connection, PreparedStatement, SQLException, Timestamp}
import java.util.Date

import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferBrokers
import sample.Tdl
import spark.dataProcess.StoreMovieEssay.regx
import spark.dto._
import utils.{CommomConfig, CommonUtil, MysqlUtil}

import scala.collection.mutable.ArrayBuffer

/**
  * feng
  * 18-10-9
  */
object ProcessMysqlData extends Logging {
  val batchInterval: Int = 2

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("ProcessMysqlData")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.streaming.backpressure.enabled", "true")
      .config("spark.streaming.blockInterval", "2s  ")
      .config("spark.defalut.parallelism", "6")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    //设置当前为测试环境
    CommonUtil.setTestEvn

    // config kafka
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> CommonUtil.getKafkaServers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "ProcessMysqlDataGroup",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )
    val topics: Array[String] = getMysqlTopic
    //config spark Streaming
    val ssc = new StreamingContext(spark.sparkContext, Seconds(batchInterval))
    ssc.checkpoint(CommonUtil.getCheckpointDir)

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferBrokers,
      Subscribe[String, String](topics, kafkaParams)
    )

    processData(spark, stream)

    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 把记录存入Phoenix
    *
    * @param spark
    * @param stream
    */
  def processData(spark: SparkSession, stream: InputDStream[ConsumerRecord[String, String]]): Unit = {
    //按表名分组(tableame,Array(tableinfo))
    val batchTableRecordDS = stream.filter(r => r.value != null).map(record => convertTableJsonStr(record.value))
      .reduceByKey(_ ++ _)

    batchTableRecordDS.foreachRDD {
      rdd: RDD[(String, ArrayBuffer[String])] =>
        if (!rdd.isEmpty()) {
          // 按partition遍历
          rdd.foreachPartition {
            part =>
              //get Connection in each partition
              val con: Connection = CommonUtil.getPhoenixConnection
              part.foreach {
                info =>
                  // 对每个记录
                  info._1 match {
                    case "tbl" => saveTbl(info, con)
                    case "steaming_record" => saveSteamingRecord(info, con)
                    case "doulist" => saveDoulist(info,con)
                    case "doulist_movie_detail" => saveDoulistMovieDetail(info,con)
                    case "film_critics" => saveFilmCritics(info,con)
                    case "movie_base_info" => saveMovieBaseInfo(info,con)
                    case "movie_detail" => saveMovieDetail(info,con)
                    case "movie_essay" => saveMovieEssay(info,con)
                    case _ => logInfo("---------not match-----")
                  }
              }
              MysqlUtil.colseConnection(con)
          }
        } else {
          logInfo("--------ProcessMysqlData empty--------")
        }
        //强制runjob
        rdd.count()
    }

  }

  def saveTbl(info: (String, ArrayBuffer[String]), con: Connection): Unit = {
    val list: ArrayBuffer[Tdl] = info._2.map(t => JSON.parseObject(t, classOf[Tdl]))
    val sql = "upsert into tbl(id,title,author) VALUES (?,?,?)"
    logInfo(sql)
    logInfo(list(0).toString)
    logInfo("insert Tdl size:" + list.size)
    val pstmt: PreparedStatement = con.prepareStatement(sql)
    list.foreach {
      r =>
        pstmt.setInt(1, r.id)
        pstmt.setString(2, r.title)
        pstmt.setString(3, r.author)
        pstmt.addBatch()
    }
    executeAndCommit(pstmt, con)
  }

  def saveSteamingRecord(info: (String, ArrayBuffer[String]), con: Connection): Unit = {
    logInfo("ArrayBuffer[String]:" + info._2(0).toString)
    val list: ArrayBuffer[SteamingRecord] = info._2.map(t => JSON.parseObject(t, classOf[SteamingRecord]))
    val executeSql = "upsert into STEAMING_RECORD(ID,STARTTIME,ENDTIME,RECORDCOUNT,RECORDTYPE,BATCHRECORDID,CREATEDTIME) " +
      "values(?,CONVERT_TZ(?, 'UTC', 'Asia/Shanghai'),CONVERT_TZ(?, 'UTC', 'Asia/Shanghai'),?,?,?," +
      "CONVERT_TZ(CURRENT_DATE(), 'UTC', 'Asia/Shanghai'))"
    var pstmt: PreparedStatement = con.prepareStatement(executeSql)
    logInfo(executeSql)
    logInfo(list(0).toString)
    logInfo("insert steaming_record size:" + list.size)
    list.foreach {
      r =>
        pstmt.setString(1, r.id)
        pstmt.setTimestamp(2, r.startTime)
        pstmt.setTimestamp(3, r.endTime)
        pstmt.setLong(4, r.recordCount)
        pstmt.setString(5, r.recordType)
        pstmt.setString(6, r.batchRecordId)
        pstmt.addBatch()
    }
    executeAndCommit(pstmt, con)
  }

  def saveDoulist(info: (String, ArrayBuffer[String]), con: Connection): Unit = {
    val list: ArrayBuffer[Doulist] = info._2.map(t => JSON.parseObject(t, classOf[Doulist]))
    val sql = "upsert INTO doulist(id,movieid,doulist_url,doulist_name,doulist_intr,user_name,user_url," +
      "collect_num,recommend_num,movie_num,doulist_cratedDate,doulist_updatedDate,created_time)" +
      "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,CONVERT_TZ(CURRENT_DATE(), 'UTC', 'Asia/Shanghai'))"
    logInfo(sql)
    logInfo(list(0).toString)
    logInfo("insert doulist size:" + list.size)
    val pstmt: PreparedStatement = con.prepareStatement(sql)
    list.foreach {
      r =>
        pstmt.setInt(1, r.id)
        pstmt.setString(2, r.movieid)
        pstmt.setString(3, r.doulistUrl)
        pstmt.setString(4, r.doulistName)
        pstmt.setString(5, r.doulistIntr)
        pstmt.setString(6, r.userName)
        pstmt.setString(7, r.userUrl)
        pstmt.setInt(8, r.collectNum)
        pstmt.setInt(9, r.recommendNum)
        pstmt.setInt(10, r.movieNum)
        pstmt.setDate(11, r.doulistCratedDate)
        pstmt.setDate(12, r.doulistUpdatedDate)
        pstmt.addBatch()
    }
    executeAndCommit(pstmt, con)
  }

  def saveDoulistMovieDetail(info: (String, ArrayBuffer[String]), con: Connection): Unit = {
    val list: ArrayBuffer[DoulistMovieDetail] = info._2.map(t => JSON.parseObject(t, classOf[DoulistMovieDetail]))
    val sql = "upsert INTO doulist_movie_detail(id,movieid,doulist_url,created_time)VALUES (?,?,?,CONVERT_TZ(CURRENT_DATE(), 'UTC', 'Asia/Shanghai'))"
    logInfo(sql)
    logInfo(list(0).toString)
    logInfo("insert DoulistMovieDetail size:" + list.size)
    val pstmt: PreparedStatement = con.prepareStatement(sql)
    list.foreach {
      r =>
        pstmt.setInt(1, r.id)
        pstmt.setString(2, r.movieid)
        pstmt.setString(3, r.doulistUrl)
        pstmt.addBatch()
    }
    executeAndCommit(pstmt, con)
  }

  def saveFilmCritics(info: (String, ArrayBuffer[String]), con: Connection): Unit = {
    val list: ArrayBuffer[FilmCritics] = info._2.map(t => JSON.parseObject(t, classOf[FilmCritics]))
    val sql = "upsert INTO film_critics(id,movieid,film_critics_url,title,user_name,user_url," +
      "comment_rate,comment_time,useless_num,useful_num,like_num,recommend_num,review,created_time)" +
      "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,CONVERT_TZ(CURRENT_DATE(), 'UTC', 'Asia/Shanghai'))"
    logInfo(sql)
    logInfo(list(0).toString)
    logInfo("insert FilmCritics size:" + list.size)
    val pstmt: PreparedStatement = con.prepareStatement(sql)
    list.foreach {
      r =>
        pstmt.setInt(1, r.id)
        pstmt.setString(2, r.movieid)
        pstmt.setString(3, r.filmCriticsUrl)
        pstmt.setString(4, r.title)
        pstmt.setString(5, r.userName)
        pstmt.setString(6, r.userUrl)
        pstmt.setBigDecimal(7, r.commentRate)
        pstmt.setDate(8, r.commentTime)
        pstmt.setInt(9, r.uselessNum)
        pstmt.setInt(10, r.usefulNum)
        pstmt.setInt(11, r.likeNum)
        pstmt.setInt(12, r.recommendNum)
        pstmt.setString(13, r.review)
        pstmt.addBatch()
    }
    executeAndCommit(pstmt, con)
  }


  def saveMovieBaseInfo(info: (String, ArrayBuffer[String]), con: Connection): Unit = {
    val list: ArrayBuffer[MovieBaseInfo] = info._2.map(t => JSON.parseObject(t, classOf[MovieBaseInfo]))
    val sql = "upsert INTO movie_base_info(id,movieid,movie_name,view_date,personal_rate,personal_tags,intro,isViewed,created_time)" +
      "VALUES(?,?,?,?,?,?,?,?,CONVERT_TZ(CURRENT_DATE(), 'UTC', 'Asia/Shanghai'))"
    logInfo(sql)
    logInfo(list(0).toString)
    logInfo("insert MovieBaseInfo size:" + list.size)
    val pstmt: PreparedStatement = con.prepareStatement(sql)
    list.foreach {
      r =>
        pstmt.setInt(1, r.id)
        pstmt.setString(2, r.movieid)
        pstmt.setString(3, r.movieName)
        pstmt.setDate(4, r.viewDate)
        pstmt.setInt(5, r.personalRate)
        pstmt.setString(6, r.personalTags)
        pstmt.setString(7, r.intro)
        pstmt.setString(8, r.isViewed)
        pstmt.addBatch()
    }
    executeAndCommit(pstmt, con)
  }

  def saveMovieDetail(info: (String, ArrayBuffer[String]), con: Connection): Unit = {
    val list: ArrayBuffer[MovieDetail] = info._2.map(t => JSON.parseObject(t, classOf[MovieDetail]))
    val sql = "upsert INTO movie_detail(id,movieid,movie_url,movie_name,director,writers,stars,genres,country," +
      "official_site,language,release_date,also_known_as,runtime,IMDb_url,douban_rate,rate_num," +
      "star_5,star_4,star_3,star_2,star_1,comparison_1,comparison_2,tags,storyline," +
      "also_like_1_name,also_like_1_url,also_like_2_name,also_like_2_url,also_like_3_name,also_like_3_url," +
      "also_like_4_name,also_like_4_url,also_like_5_name,also_like_5_url,also_like_6_name,also_like_6_url," +
      "also_like_7_name,also_like_7_url,also_like_8_name,also_like_8_url,also_like_9_name,also_like_9_url," +
      "also_like_10_name,also_like_10_url,essay_collect_url,film_critics_url,doulists_url,viewed_num," +
      "want_to_view_num,image_url,created_time)" +
      "VALUES(?,?,?,?,?,?,?,?,?,?, ?,?,?,?,?,?,?,?,?,?, ?,?,?,?,?,?,?,?,?,?, ?,?,?,?,?,?,?,?,?,?, " +
      "?,?,?,?,?,?,?,?,?,?, ?,?,CONVERT_TZ(CURRENT_DATE(), 'UTC','Asia/Shanghai'))"
    logInfo(sql)
    logInfo(list(0).toString)
    logInfo("insert MovieDetail size:" + list.size)
    val pstmt: PreparedStatement = con.prepareStatement(sql)
    list.foreach {
      r =>
        pstmt.setInt(1, r.id)
        pstmt.setString(2, r.movieid)
        pstmt.setString(3, r.movieUrl)
        pstmt.setString(4, r.movieName)
        pstmt.setString(5, r.director)
        pstmt.setString(6, r.writers)
        pstmt.setString(7, r.stars)
        pstmt.setString(8, r.genres)
        pstmt.setString(9, r.country)
        pstmt.setString(10, r.officialSite)
        pstmt.setString(11, r.language)
        pstmt.setString(12, r.releaseDate)
        pstmt.setString(13, r.alsoKnown_as)
        pstmt.setString(14, r.runtime)
        pstmt.setString(15, r.IMDbUrl)
        pstmt.setBigDecimal(16, r.doubanRate)
        pstmt.setInt(17, r.rateNum)
        pstmt.setString(18, r.star5)
        pstmt.setString(19, r.star4)
        pstmt.setString(20, r.star3)
        pstmt.setString(21, r.star2)
        pstmt.setString(22, r.star1)
        pstmt.setString(23, r.comparison1)
        pstmt.setString(24, r.comparison2)
        pstmt.setString(25, r.tags)
        pstmt.setString(26, r.storyline)
        pstmt.setString(27, r.alsoLike1Name)
        pstmt.setString(28, r.alsoLike1Url)
        pstmt.setString(29, r.alsoLike2Name)
        pstmt.setString(30, r.alsoLike2Url)
        pstmt.setString(31, r.alsoLike3Name)
        pstmt.setString(32, r.alsoLike3Url)
        pstmt.setString(33, r.alsoLike4Name)
        pstmt.setString(34, r.alsoLike4Url)
        pstmt.setString(35, r.alsoLike5Name)
        pstmt.setString(36, r.alsoLike5Url)
        pstmt.setString(37, r.alsoLike6Name)
        pstmt.setString(38, r.alsoLike6Url)
        pstmt.setString(39, r.alsoLike7Name)
        pstmt.setString(40, r.alsoLike7Url)
        pstmt.setString(41, r.alsoLike8Name)
        pstmt.setString(42, r.alsoLike8Url)
        pstmt.setString(43, r.alsoLike9Name)
        pstmt.setString(44, r.alsoLike9Url)
        pstmt.setString(45, r.alsoLike10Name)
        pstmt.setString(46, r.alsoLike10Url)
        pstmt.setString(47, r.essayCollectUrl)
        pstmt.setString(48, r.filmCriticsUrl)
        pstmt.setString(49, r.doulistsUrl)
        pstmt.setInt(50, r.viewedNum)
        pstmt.setInt(51, r.wantToViewNum)
        pstmt.setString(52, r.imageUrl)
        pstmt.addBatch()
    }
    executeAndCommit(pstmt, con)
  }

  def saveMovieEssay(info: (String, ArrayBuffer[String]), con: Connection): Unit = {
    val list: ArrayBuffer[MovieEssay] = info._2.map(t => JSON.parseObject(t, classOf[MovieEssay]))
    val sql = "upsert INTO movie_essay(id,movieid,user_name,user_url,comment,comment_rate,comment_time,created_time)" +
      "VALUES(?,?,?,?,?,?,?,CONVERT_TZ(CURRENT_DATE(), 'UTC', 'Asia/Shanghai'))"
    logInfo(sql)
    logInfo(list(0).toString)
    logInfo("insert MovieEssay size:" + list.size)
    val pstmt: PreparedStatement = con.prepareStatement(sql)
    list.foreach {
      r =>
        pstmt.setInt(1, r.id)
        pstmt.setString(2, r.movieid)
        pstmt.setString(3, r.userName)
        pstmt.setString(4, r.userUrl)
        pstmt.setString(5, r.comment)
        pstmt.setString(6, r.commentRate)
        pstmt.setDate(7, r.commentTime)
        pstmt.addBatch()
    }
    executeAndCommit(pstmt, con)
  }

  def executeAndCommit(ps: PreparedStatement, con: Connection) = {
    try {
      ps.executeBatch()
      con.commit()
    } catch {
      case e: SQLException =>
        logError(e.getMessage)
    }
  }

  def convertTableJsonStr(jsonstr: String): (String, ArrayBuffer[String]) = {
    val jsonObject = JSON.parseObject(jsonstr);
    val data = jsonObject.getString("payload");
    val schema = JSON.parseObject(jsonObject.getString("schema"));
    val topicName = schema.getString("name").split("\\.")
    val tableName = topicName(topicName.size - 2)
    (tableName, ArrayBuffer(data))
  }

  def getMysqlTopic: Array[String] = {
    var topics = Array("mysqlfullfillment.test.steaming_record", "mysqlfullfillment.test.tbl")
    if (CommomConfig.isTest) {
      topics = Array("mysqlfullfillment.test.steaming_record", "mysqlfullfillment.test.tbl")
    }
    logInfo("topic:" + topics)
    topics
  }
}
