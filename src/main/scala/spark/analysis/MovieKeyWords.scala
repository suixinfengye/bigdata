package spark.analysis

import java.sql.Connection

import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import spark.dataProcess.ProcessMysqlData.logInfo
import spark.dto.Review
import utils.{AnsjUtils, CommonUtil, MysqlUtil}

/**
  * https://my.oschina.net/uchihamadara/blog/2032481
  * ./bin/spark-submit \
  * --class spark.analysis.MovieKeyWords \
  * --master spark://spark1:7077 \
  * --executor-memory 1G \
  * --num-executors 2 \
  * --total-executor-cores 2 \
  * --files "/usr/local/userlib/spark-2.2/conf/log4j-executor.properties" \
  * --driver-java-options "-Dlog4j.debug=true -Dlog4j.configuration=log4j.properties" \
  * --conf "spark.executor.extraJavaOptions=-Dlog4j.debug=true -Dlog4j.configuration=log4j-executor.properties -XX:+PrintGCDetails -Xloggc:/usr/local/userlib/spark-2.2/logs/executor_gc.log -XX:+PrintGCDateStamps -XX:+PrintHeapAtGC -XX:+UseG1GC -XX:+PrintTenuringDistribution -Xms400m -XX:+PrintCommandLineFlags -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/usr/local/userlib/spark-2.2/logs/executor_oom.hprof" \
  * /usr/local/userlib/jars/bigdata.jar
  * feng
  * 19-1-10
  */
object MovieKeyWords extends Logging {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("MovieKeyWords")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    computeMovieKeyWords(spark)
    readComment(spark)

    spark.stop()
  }

  def computeMovieKeyWords(spark: SparkSession): Unit = {
    import spark.implicits._
    val conf = CommonUtil.getReadHbaseConfig("review")
    val hbaseRDD = spark.sparkContext.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

    //movieid,contents
    val reveiwRDD = hbaseRDD.map {
      case (_, result) =>
        //获取行键 eg:10001418-5584520
        val key = Bytes.toString(result.getRow).split("-")
        //通过列族和列名获取列
        val content = Bytes.toString(result.getValue("cf".getBytes, "cn".getBytes))
        //movieid,content
        (key(0), content)
    }.reduceByKey(_ + _)

    //convert to row
    val movieid2ReviewsRDD = reveiwRDD.map {
      case (movieid: String, contents: String) =>
        Row(movieid, contents)
    }
    val schema = StructType(Array(StructField("moiveid", StringType, false), StructField("content", StringType, false)))
    val reveiwDF = spark.createDataFrame(movieid2ReviewsRDD, schema)

    val ansj = new AnsjUtils
    val bansj = spark.sparkContext.broadcast(ansj)
    val convert = reveiwDF.map {
      case Row(movieid: String, content: String) => {
        val ansjInstance = bansj.value
        val keyWords = ansjInstance.getKeywords(content).toString
        (movieid, keyWords)
      }
    }
    convert.printSchema()
    convert.take(10).foreach(print(_))
  }

  /**
    * 读取短评
    *
    * @param spark
    * @return
    */
  def readComment(spark: SparkSession): DataFrame = {
    val commentDF = CommonUtil.readPhoenixTable(spark, "movie_essay")
//    commentDF.printSchema()
//    commentDF.show()

//    commentDF.select("MOVIEID","COMMENT").groupBy("MOVIEID").agg("MOVIEID",)
//    commentDF

    commentDF.createOrReplaceTempView("essaytmp")
    spark.sql("select MOVIEID,aggComments(COMMENT) from essaytmp group by MOVIEID")



  }
}
