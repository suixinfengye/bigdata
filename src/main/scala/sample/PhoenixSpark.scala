package sample

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.phoenix.spark._
import org.apache.spark.rdd.RDD

object PhoenixSpark {
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      //            .master("local")
      .appName("PhoenixSpark")
      .getOrCreate()
    //    val dataSet = List((1L, "1", 1), (2L, "2", 2), (3L, "3", 3))
    val dataSet = List(Aa(1), Aa(2))
    val a:RDD[Aa] = spark.sparkContext
      .parallelize(dataSet)
    a.saveToPhoenix(
        "OUTPUT_TEST_TABLE",
        Seq("ID", "COL1", "COL2"),
        zkUrl = Some("phoenix-server:2181")
      )

    //    spark.read.format("org.apache.phoenix.spark").option("table" ,"WEB_STAT").option("zkUrl", "localhost:2181").load().show()
    spark.read.format("org.apache.phoenix.spark").option("table", "WEB_STAT").option("zkUrl", "spark1:2181").load().show()
    spark.stop()
  }
}

case class Aa(a: Int)
