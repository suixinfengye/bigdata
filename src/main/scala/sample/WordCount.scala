package sample

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object WordCount {
  val logger = LoggerFactory.getLogger(this.getClass)
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("WordCount")
      .getOrCreate()
//    val file = spark.sparkContext.textFile("hdfs://localhost:9000/test/common")
//    file.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).collect().foreach(println(_))



    spark.sparkContext.makeRDD(List(1,2,3,4,5,6)).collect().foreach(println(_))
    logger.error("-------------------sdfhdhhd")
    spark.stop()
  }
}
