package spark.analysis

import java.sql.Connection

import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import spark.dataProcess.ProcessMysqlData.logInfo
import spark.dto.Review
import utils.{AnsjUtils, CommonUtil, MysqlUtil}

/**
  * https://my.oschina.net/uchihamadara/blog/2032481
  * ./bin/spark-submit \
  * --class spark.analysis.MovieKeyWords \
  * --master spark://spark1:7077 \
  * --executor-memory 1G \
  * --num-executors 3 \
  * --total-executor-cores 3 \
  * --files "/usr/local/userlib/spark-2.2/conf/log4j-executor.properties" \
  * --driver-java-options "-Dlog4j.debug=true -Dlog4j.configuration=log4j.properties -XX:+HeapDumpOnOutOfMemoryError
  * -XX:HeapDumpPath=/usr/local/userlib/spark-2.2/logs/driver_oom.hprof
  * -XX:+PrintGCDetails -Xloggc:/usr/local/userlib/spark-2.2/logs/driver_gc.log -XX:+PrintGCDateStamps -XX:+PrintHeapAtGC
  * -XX:+PrintGCApplicationConcurrentTime -XX:+PrintGCApplicationStoppedTime" \
  * --conf "spark.executor.extraJavaOptions=-Dlog4j.debug=true -Dlog4j.configuration=log4j-executor.properties -XX:+PrintGCDetails
  * -Xloggc:/usr/local/userlib/spark-2.2/logs/executor_gc.log -XX:+PrintGCDateStamps -XX:+PrintHeapAtGC -XX:+UseG1GC
  * -XX:+PrintTenuringDistribution -Xms400m -XX:+PrintCommandLineFlags -XX:+HeapDumpOnOutOfMemoryError
  * -XX:HeapDumpPath=/usr/local/userlib/spark-2.2/logs/executor_oom.hprof" \
  * /usr/local/userlib/jars/bigdata.jar
  * feng
  * 19-1-10
  */
object MovieKeyWords extends Logging {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("MovieKeyWords")
      //determines the 'default number of partitions in RDDs returned by transformations like join,
      //reduceByKey, and parallelize when not set by user
      .config("spark.defalut.parallelism", "6") //rdd
      //Configures the number of partitions to use when shuffling data for joins or aggregations.
      .config("spark.sql.shuffle.partitions", "200") //dataframe
      .config("spark.locality.wait", "1s")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val reviewDF = readReview(spark)
    val commentDF = readComment(spark)

    computeMovieKeyWords(spark, reviewDF)
    computeMovieKeyWords(spark, commentDF)

    //这一步可以试试提高并行度
    //    val unionDF = reviewDF.repartition(24).join(commentDF.repartition(24))
    val unionDF = reviewDF.join(commentDF, "movieid")
    computeMovieKeyWords(spark, unionDF)

    spark.stop()
  }

  /**
    * 读取 影评
    *
    * @param spark
    */
  def readReview(spark: SparkSession): DataFrame = {
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
    val schema = StructType(Array(StructField("movieid", StringType, false), StructField("content", StringType, false)))
    val reveiwDF = spark.createDataFrame(movieid2ReviewsRDD, schema).persist(StorageLevel.MEMORY_AND_DISK_SER)
    reveiwDF.printSchema()
    reveiwDF.take(10).foreach(print(_))
    reveiwDF
  }

  /**
    * 读取短评
    *
    * @param spark
    * @return
    */
  def readComment(spark: SparkSession): DataFrame = {
    import spark.implicits._
    val commentDF = CommonUtil.readPhoenixTable(spark, "movie_essay")
    commentDF.createOrReplaceTempView("essaytmp")
    //     定义和注册自定义函数
    spark.udf.register("aggComments", new AggComments)
    val movieCommentDF: DataFrame = spark.sql("select MOVIEID as movieid, aggComments(COMMENT) as content from essaytmp " +
      "group by MOVIEID").persist(StorageLevel.MEMORY_AND_DISK_SER)

    movieCommentDF.take(10).foreach(print(_))
    movieCommentDF.show()
    movieCommentDF
  }

  def computeMovieKeyWords(spark: SparkSession, df: DataFrame): Unit = {
    import spark.implicits._
    val ansj = new AnsjUtils
    val bansj = spark.sparkContext.broadcast(ansj)
    val convert = df.map {
      case Row(movieid: String, content: String) => {
        val ansjInstance = bansj.value
        val keyWords = ansjInstance.getKeywords(content).toString
        (movieid, keyWords)
      }
    }
    convert.printSchema()
    convert.take(10).foreach(print(_))
  }
}
