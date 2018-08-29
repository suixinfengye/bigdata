package sample

import org.apache.spark.sql.SparkSession

object WordCount {

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
//      .master("local")
      .appName("WordCount")
      .getOrCreate()
    import spark.implicits._
    val file = spark.sparkContext.textFile("hdfs://localhost:9000/test/common")
    file.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).collect().foreach(println(_))

    spark.stop()
  }
}
