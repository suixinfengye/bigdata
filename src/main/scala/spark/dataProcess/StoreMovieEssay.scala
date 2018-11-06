package spark.dataProcess


import java.sql.Timestamp
import java.util.Date

import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.phoenix.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.util.LongAccumulator
import spark.dto.{Review, SteamingRecord}
import utils.{CommomConfig, CommonUtil, MyConstant, MyDateUtil}

import scala.collection.mutable.ListBuffer


/**
  * 整合flume kafka hbase spark-streaming 将文章内容解析后储存于hbase
  * hbase key为 reverse(movieid)-reviewid
  *
  * ./kafka-topics.sh --create --zookeeper 192.168.0.101:2181,192.168.0.107:2181,192.168.0.108:2181
  * --replication-factor 2 --partitions 6 --topic movie-essay-topic
  * bin/flume-ng agent -n logser -c conf -f conf/flume_test.conf
  *
  ./bin/spark-submit \
  --class spark.dataProcess.StoreMovieEssay \
  --master spark://feng:7077 \
  --executor-memory 1G \
  --num-executors 3 \
  --total-executor-cores 3 \
  /home/feng/software/code/bigdata/out/artifacts/bigdata/bigdata.jar
  * feng
  * 18-9-24
  */
object StoreMovieEssay extends Logging {

  val batchInterval: Int = 6

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      //      .master("local")
      .appName("StoreMovieEssay")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.streaming.backpressure.enabled", "true")
      .config("spark.streaming.kafka.maxRatePerPartition", 1000)
      .config("spark.streaming.blockInterval", "3s")
      .config("spark.defalut.parallelism", "6")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    //设置当前为测试环境
    //    CommonUtil.setTestEvn

    /**
      * If your Spark batch duration is larger than the default Kafka heartbeat session timeout (30 seconds),
      * increase heartbeat.interval.ms and session.timeout.ms appropriately. For batches larger than 5 minutes,
      * this will require changing group.max.session.timeout.ms on the broker.
      */
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> CommonUtil.getKafkaServers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "StoreMovieEssayGroup",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )
    val topics = Array(getTopic)

    //config spark Streaming
    val ssc = new StreamingContext(spark.sparkContext, Seconds(batchInterval))

    /**
      * If you enable Spark checkpointing, offsets will be stored in the checkpoint.
      * 缺点:1.output operation must be idempotent, since you will get repeated outputs;
      * 2.transactions are not an option.
      * 3.you cannot recover from a checkpoint if your application code has changed.
      */
    ssc.checkpoint(CommonUtil.getCheckpointDir)
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,

      /**
        * PreferConsistent:distribute partitions evenly across available executors
        * PreferBrokers:executors are on the same hosts as Kafka brokers,
        *   which will prefer to schedule partitions on the Kafka leader for that partition
        * PreferFixed:数据有倾斜时,will specify an explicit mapping of partitions to hosts
        */
      LocationStrategies.PreferBrokers,
      Subscribe[String, String](topics, kafkaParams)
    )

    val acc: LongAccumulator = spark.sparkContext.longAccumulator("movieCount")
    val startTime = new Timestamp(new Date().getTime())

    processData(spark, stream, acc)

    spark.sparkContext.addSparkListener(new MovieEssaySparkListener(spark, acc, startTime))

    ssc.start()
    ssc.awaitTermination()

  }


  /**
    * 把记录存入hbase
    *
    * @param spark
    * @param stream
    */
  def processData(spark: SparkSession, stream: InputDStream[ConsumerRecord[String, String]], acc: LongAccumulator): Unit = {
    val batchRecordDS = stream.map(mapFunc = record => {
      if (StringUtils.isNotEmpty(record.value())) {
        regx(record.value)
      } else {
        List.empty
      }
    }).filter(list => list.nonEmpty).cache()
    saveIntoHbase(batchRecordDS, spark, acc)
    streamingOpr(batchRecordDS, spark)
  }

  /**
    * 把记录存入hbase
    *
    * @param batchRecordDS
    * @param spark
    */
  def saveIntoHbase(batchRecordDS: DStream[List[Review]], spark: SparkSession, acc: LongAccumulator): Unit = {

    //save into hbase
    batchRecordDS.foreachRDD {
      r => //List[List[Review]]
        if (!r.isEmpty()) {
          val reviewList: List[Review] = r.filter(list => list.nonEmpty).reduce(_ ++ _)
          logInfo("save reviewList to hbase:" + reviewList.size)
          acc.add(reviewList.size)
          val jobConf = CommonUtil.getWriteHbaseConfig(spark, getTableName)
          spark.sparkContext.parallelize(reviewList).map(converToPut).saveAsHadoopDataset(jobConf)
        }
        logInfo("rdd[list[list[Review]]] is empty" + r.id)
    }
  }

  /**
    * 统计每个窗口期间入库总数,movie 评论入库数,放入phoenix表中
    *
    * recordType: MED(movie eassy Duration),一个长期间内的入库数
    * MEDM(movie eassy Duration for a movie),一个长期间内某个电影的影评入库数
    * batchRecordId : 某个recordType具体类型的标识符,如movieId
    */
  def streamingOpr(batchRecordDS: DStream[List[Review]], spark: SparkSession): Unit = {
    import spark.implicits._
    val movieWindowRecords: DStream[(String, List[Review])] = batchRecordDS.window(Seconds(batchInterval * 3), Seconds
    (batchInterval * 3)).map {
      reviews: List[Review] =>
        val key = reviews(0).movieid
        (key, reviews)
    }.reduceByKey(_ ++ _)

    val recordAgg = movieWindowRecords.mapPartitions {
      iter: Iterator[(String, List[Review])] =>
        val date = new Date
        val dateStr = MyDateUtil.dateFormat(date)
        val curretTime = new Timestamp(date.getTime)
        val recordType = MyConstant.RECORD_TYPE_MEDM
        //t:(String, List[Review])
        //主键设置为 movieid+timestamp, 每一个批次处理时,都是按movieid分组的,一个批次同一个movieid只会有一个
        iter.map { t =>
          val s = SteamingRecord(t._1 + dateStr, null, curretTime, t._2.size, recordType, t._1, curretTime, null)
          logInfo("-----+++:" + s.toString)
          s
        }
    }

    logInfo("start to save into Phoenix")
    //入库
    recordAgg.foreachRDD {
      t: RDD[SteamingRecord] =>
        if (!t.isEmpty()) {
          val conf = CommonUtil.getHbaseConfig
          t.toDF().saveToPhoenix("STEAMING_RECORD", conf, Option(CommonUtil.getZkurl))
        }
    }
  }

  /**
    * 解析生成影评对象集合
    *
    * @param stringContent
    * @return
    */
  def regx(stringContent: String): List[Review] = {
    val list = stringContent.split("---==---")
    val pattern = "([0-9]{5,})".r
    val fileNamePattern = "/bigdata/test/"
    val reviewPattern = "https://movie.douban.com/review/"
    val contentPattern = ":::"
    val reviewList: ListBuffer[Review] = ListBuffer[Review]()
    if (list == null) {
      return reviewList.toList
    }
    list.foreach { item =>
      if (item.length > 90) { //过滤空对象
        val fileNameIndex: Int = item.indexOf(fileNamePattern)
        val reviewIndex: Int = item.indexOf(reviewPattern)
        val contentIndex: Int = item.indexOf(contentPattern)
        val movieid = pattern findFirstIn item.substring(fileNameIndex + 14, reviewIndex)
        val reviewid = item.substring(reviewIndex + 32, contentIndex - 1)
        val content = item.substring(contentIndex + 3).trim
        reviewList.append(Review(movieid.get, reviewid, content))
      } else {
        logInfo("empty review:" + item)
      }
    }
    reviewList.toList
  }

  /**
    * 转换成 put
    *
    * @param c
    * @return
    */
  def converToPut(c: Review) = {
    // rowkey设计 均衡分布记录
    val put = new Put(Bytes.toBytes(c.movieid + "-" + c.reviewid))
    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("cn"), Bytes.toBytes(c.content))
    (new ImmutableBytesWritable, put)
  }

  //create 'review', 'cf'
  def getTableName: String = {
    var tableName = "review"
    if (CommomConfig.isTest) {
      tableName = "test"
    }
    logInfo("tableName:" + tableName)
    tableName
  }

  def getTopic: String = {
    var topics: String = "movie-essay-topic"
    if (CommomConfig.isTest) {
      topics = "test-flume-topic"
    }
    logInfo("topic:" + topics)
    topics
  }
}
