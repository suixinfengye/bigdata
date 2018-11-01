package sample

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
//./bin/spark-submit \
//--class sample.WordCount \
//--master spark://spark1:7077 \
//--executor-memory 1G \
//--total-executor-cores 2 \
///usr/local/userlib/jars/bigdata.jar
object WordCount {
  val logger = LoggerFactory.getLogger(this.getClass)
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
//      .master("local")
      .appName("WordCount")
      .getOrCreate()
//    val file = spark.sparkContext.textFile("hdfs://localhost:9000/test/common")
//    file.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).collect().foreach(println(_))



    spark.sparkContext.makeRDD(List(1,2,3,4,5,6)).collect().foreach(println(_))
    logger.info("-------------------sdfhdhhd")
    spark.stop()
  }
}
