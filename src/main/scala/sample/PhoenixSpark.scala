package sample

import java.util.Date

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.phoenix.spark._
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory
import utils.{MyConstant, MyDateUtil}

object PhoenixSpark {

  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("PhoenixSpark")
//      .config("mapreduce.output.fileoutputformat.outputdir","hdfs://localhost:9000/tmp/mapreduceOutput")
      .getOrCreate()

    val date = new Date
    val dateStr = MyDateUtil.dateFormat(date)
    val recordType = MyConstant.RECORD_TYPE_MED

    val dataSet = List(("456541177", dateStr, 45, recordType, date))

    //    SteamingRecord("MovieEssay" + dateStr, dateStr, acc.value, recordType, t._1)
    //    val dataSet = List(Aa(1), Aa(2))
    val a = spark.sparkContext
      .parallelize(dataSet)
//    a.collect().foreach(t=>logger.info("-------------"+t.toString()))
    a.saveToPhoenix(
      "STEAMING_RECORD",
      Seq("ID", "TIME", "RECORDCOUNT", "RECORDTYPE", "CREATED_TIME"),
      zkUrl = Some("localhost:2181")
    )

    //    spark.read.format("org.apache.phoenix.spark").option("table" ,"WEB_STAT").option("zkUrl", "localhost:2181").load().show()
    //    spark.read.format("org.apache.phoenix.spark").option("table", "WEB_STAT").option("zkUrl", "spark1:2181").load().show()
    spark.stop()
  }
}

case class Aa(a: Int)
