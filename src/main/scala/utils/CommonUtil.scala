package utils

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
    var zookeeperQuorum = _
    if (CommomConfig.isTest) {
      zookeeperQuorum = CommomConfig.HBASE_ZOOKEEPER_QUORUM_TEST
    } else {
      zookeeperQuorum = CommomConfig.HBASE_ZOOKEEPER_QUORUM
    }
    logger.info("zookeeperQuorum is :" + zookeeperQuorum)
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181"); //设置zookeeper client端口
    conf.set("hbase.zookeeper.quorum", zookeeperQuorum)
    val jobConf = new JobConf(conf, this.getClass)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    jobConf
  }

  def getKafkaServers(): String = {
    var kafkaServers = _
    if (CommomConfig.isTest) {
      kafkaServers = CommomConfig.BOOTSTRAP_SERVERS_TEST
    } else {
      kafkaServers = CommomConfig.BOOTSTRAP_SERVERS
    }
    logger.info("kafkaServers is :" + kafkaServers)
    kafkaServers
  }

  def getCheckpointDir(): String = {
    var checkpointDir = _
    if (CommomConfig.isTest) {
      checkpointDir = CommomConfig.CHECKPOINT_DIR_LOCAL_TEST
    } else {
      checkpointDir = CommomConfig.CHECKPOINT_DIR
    }
    logger.info("checkpointDir is :" + checkpointDir)
    checkpointDir
  }
}
