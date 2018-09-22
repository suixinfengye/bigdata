package sample

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.phoenix.spark._
object PhoenixSpark {
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
//            .master("local")
      .appName("PhoenixSpark")
      .getOrCreate()


//    spark.read.format("org.apache.phoenix.spark").option("table" ,"WEB_STAT").option("zkUrl", "localhost:2181").load().show()
    spark.read.format("org.apache.phoenix.spark").option("table" ,"WEB_STAT").option("zkUrl", "spark1:2181").load().show()
    spark.stop()
  }
}
