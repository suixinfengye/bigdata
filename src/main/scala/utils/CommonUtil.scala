package utils

import java.sql.{Connection, DriverManager}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
  * feng
  * 18-9-29
  */
object CommonUtil {

  val logger = LoggerFactory.getLogger(this.getClass)

  /**
    * 生成Hbase config对象
    *
    * @param spark
    * @param tableName
    * @return
    */
  def getWriteHbaseConfig(spark: SparkSession, tableName: String): JobConf = {
    val conf = getHbaseConfig
    val jobConf = new JobConf(conf, this.getClass)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    jobConf
  }

  def getHbaseConfig: Configuration = {
    var zookeeperQuorum: String = CommomConfig.HBASE_ZOOKEEPER_QUORUM
    var outputdir = CommomConfig.HBASE_OUTPUTDIR
    if (CommomConfig.isTest) {
      zookeeperQuorum = CommomConfig.HBASE_ZOOKEEPER_QUORUM_TEST
      outputdir = CommomConfig.HBASE_OUTPUTDIR_TEST
    }
    logger.info("zookeeperQuorum is : " + zookeeperQuorum)

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181"); //设置zookeeper client端口
    conf.set("hbase.zookeeper.quorum", zookeeperQuorum)
    conf.set("mapreduce.output.fileoutputformat.outputdir", outputdir);
    conf
  }

  def getKafkaServers: String = {
    var kafkaServers: String = CommomConfig.BOOTSTRAP_SERVERS
    if (CommomConfig.isTest) {
      kafkaServers = CommomConfig.BOOTSTRAP_SERVERS_TEST
    }
    logger.info("kafkaServers is : " + kafkaServers)
    kafkaServers
  }

  def getCheckpointDir: String = {
    var checkpointDir: String = CommomConfig.CHECKPOINT_DIR
    if (CommomConfig.isTest) {
      checkpointDir = CommomConfig.CHECKPOINT_DIR_LOCAL_TEST
    }
    logger.info("checkpointDir is : " + checkpointDir)
    checkpointDir
  }

  def getZkurl: String = {
    var zkurl = CommomConfig.ZK_URL
    if (CommomConfig.isTest) {
      zkurl = CommomConfig.ZK_URL_TEST
    }
    logger.info("zkurl is : " + zkurl)
    zkurl
  }

  def getMysqlurl: String = {
    var mysqlUrl = CommomConfig.MYSQL_URL
    if (CommomConfig.isTest) {
      mysqlUrl = CommomConfig.MYSQL_URL_TEST
    }
    logger.info("mysqlUrl is : " + mysqlUrl)
    mysqlUrl
  }

  def getPhoenixConnection: Connection = {
    var cc: Connection = null
    val driver = CommomConfig.Phoenix_DRIVER
    val url = getPhoenixurl
    try {
      Class.forName(driver)
      cc = DriverManager.getConnection(url)
    } catch {
      case e: Exception =>
        logger.error(e.getMessage)
    }
    cc
  }

  def getRedis = {
    if (CommomConfig.isTest) {
      RedisUtil.getLocalJedis
    } else {
      RedisUtil.getJedisCluster
    }
  }

  def getPhoenixurl: String = {
    var phoenixUrl = CommomConfig.Phoenix_URL
    if (CommomConfig.isTest) {
      phoenixUrl = CommomConfig.Phoenix_URL_LOCAL
    }
    logger.info("Phoenix_URL is : " + phoenixUrl)
    phoenixUrl
  }


  /**
    * 设置当前为测试环境
    */
  def setTestEvn: Unit = {
    CommomConfig.isTest = true
  }
}
