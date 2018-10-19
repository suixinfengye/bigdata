package sample

import java.sql.Timestamp
import java.util.Date

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.phoenix.spark._
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory
import spark.dto.SteamingRecord
import utils.{CommomConfig, CommonUtil, MyConstant, MyDateUtil}

import scala.util.Random

object PhoenixSpark {

  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("PhoenixSpark")
      .getOrCreate()
    //设置当前为测试环境
    CommonUtil.setTestEvn

    dataFrameTest(spark)

    //    spark.read.format("org.apache.phoenix.spark").option("table" ,"WEB_STAT").option("zkUrl", "localhost:2181").load().show()
    //    spark.read.format("org.apache.phoenix.spark").option("table", "WEB_STAT").option("zkUrl", "spark1:2181").load().show()
    spark.stop()
  }

  def rddTest(spark: SparkSession): Unit = {
    import spark.implicits._

    val conf = HBaseConfiguration.create()
    conf.set("mapreduce.output.fileoutputformat.outputdir", "hdfs://localhost:9000/tmp/mapreduceOutput");

    val date = new Date
    val dateStr = MyDateUtil.dateFormat(date)
    val curretTime = new Timestamp(date.getTime)
    val recordType = MyConstant.RECORD_TYPE_MEDM
    //t:(String, List[Review])
    //主键设置为 movieid+timestamp, 每一个批次处理时,都是按movieid分组的,一个批次同一个movieid只会有一个
    val re = SteamingRecord(Random.nextInt(1000) + dateStr, null, curretTime, Random.nextInt(1000), recordType, Random
      .nextInt(1000) + "id", curretTime, null)
    logger.info(re.toString)

    val a = spark.sparkContext
      .parallelize(List(re))
    a.saveToPhoenix(
      "STEAMING_RECORD",
      //      Seq("ID", "TIME", "RECORDCOUNT", "RECORDTYPE", "CREATED_TIME"),
      Seq("ID", "TIME", "RECORDCOUNT", "RECORDTYPE", "BATCHRECORDID"),
      conf,
      zkUrl = Some("127.0.0.1:2181")
    )
  }

  def dataFrameTest(spark: SparkSession): Unit = {
    import spark.implicits._
    val date = new Date
    val dateStr = MyDateUtil.dateFormat(date)
    val curretTime = new Timestamp(date.getTime)
    val recordType = MyConstant.RECORD_TYPE_MEDM
//    val re = SteamingRecord(Random.nextInt(1000) + dateStr, null, curretTime, Random.nextInt(1000), recordType, Random.nextInt(1000) + "id", curretTime, null)
    val re = SteamingRecord(dateStr, null, curretTime,1, null, null, curretTime, null)
    logger.info(re.toString)
    val t = spark.createDataset(Seq(re))
    t.show()
    val conf = CommonUtil.getHbaseConfig
    t.toDF().saveToPhoenix("STEAMING_RECORD",conf,Option(CommonUtil.getZkurl))
  }

}

//http://phoenix.apache.org/tuning_guide.html

//try (Connection conn = DriverManager.getConnection(url)) {
//  conn.setAutoCommit(false);
//  int batchSize = 0;
//  int commitSize = 1000; // number of rows you want to commit per batch.
//  try (Statement stmt = conn.prepareStatement(upsert)) {
//  stmt.set ... while (there are records to upsert) {
//  stmt.executeUpdate();
//  batchSize++;
//  if (batchSize % commitSize == 0) {
//  conn.commit();
//}
//}
//  conn.commit(); // commit the last batch of records
//}

