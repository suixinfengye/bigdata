package sample

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
//./bin/spark-submit \
//--class sample.WordCount \
//--master spark://spark1:7077 \
//--executor-memory 1G \
//--files log4j.properties \
//--total-executor-cores 2 \
//--files "/usr/local/userlib/conf/log4j.properties" \
//--driver-java-options "-Dlog4j.debug=true -Dlog4j.configuration=log4j.properties" \
//--conf "spark.executor.extraJavaOptions=-Dlog4j.debug=true -Dlog4j.configuration=log4j.properties" \
///usr/local/userlib/jars/bigdata.jar
//
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
      .master("local")
      .appName("WordCount")
      .getOrCreate()
//    val file = spark.sparkContext.textFile("hdfs://localhost:9000/test/common")
//    file.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).collect().foreach(println(_))



    spark.sparkContext.makeRDD(List(1,2,3,4,5,6)).foreach(t=>logger.info("---------foreach info-------------"))
    logger.info("-----------info--------sdfhdhhd")
    logger.error("------------error-------sdfhdhhd")
    print("-----------print--------sdfhdhhd")
    spark.stop()
  }
}
