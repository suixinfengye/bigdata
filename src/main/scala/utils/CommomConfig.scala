package utils

/**
  * feng
  * 18-9-25
  */
object CommomConfig {

  val BOOTSTRAP_SERVERS = "192.168.0.101:9092,192.168.0.107:9092,192.168.0.108:9092"

  val BOOTSTRAP_SERVERS_TEST = "192.168.0.100:9092"

  val CHECKPOINT_DIR = "hdfs://spark1:9000//spark//checkpoint"

  val CHECKPOINT_DIR_LOCAL_TEST = "/home/feng/software/code/bigdata/spark-warehouse"

  val HBASE_ZOOKEEPER_QUORUM = "spark1,spark2,spark3"

  val HBASE_ZOOKEEPER_QUORUM_TEST = "localhost"

  val ZK_URL = "spark1:2181,spark2:2181,spark3:2181"

  val ZK_URL_TEST = "192.168.0.100:2181"

  //当前是否为测试环境
  var isTest = false
}
