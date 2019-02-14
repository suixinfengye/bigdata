package utils

/**
  * feng
  */
object CommomConfig {

  val BOOTSTRAP_SERVERS = "192.168.0.101:9092,192.168.0.107:9092,192.168.0.108:9092"

  val BOOTSTRAP_SERVERS_TEST = "192.168.0.100:9092"

  val CHECKPOINT_DIR = "hdfs://spark1:9000//spark//checkpoint"

  val CHECKPOINT_DIR_LOCAL_TEST = "/home/feng/software/code/bigdata/spark-warehouse"

  val HBASE_ZOOKEEPER_QUORUM = "spark1,spark2,spark3"

  val HBASE_ZOOKEEPER_QUORUM_TEST = "localhost"

  val HBASE_OUTPUTDIR_TEST = "hdfs://localhost:9000/tmp/mapreduceOutput"

  val HBASE_OUTPUTDIR = "hdfs://spark1:9000/tmp/mapreduceOutput"

//  val ZK_URL = "spark1:2181,spark2:2181,spark3:2181"
  val Phoenix_ZK_URL = "jdbc:phoenix:spark1:2181"

  val ZK_URL = "192.168.0.101:2181,192.168.0.107:2181,192.168.0.108:2181"

  val ZK_URL_TEST = "192.168.0.100:2181"

  val MYSQL_URL_TEST = "jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=utf-8&useSSL=false"

  val MYSQL_URL = "jdbc:mysql://192.168.0.100:3306/bigdata?useUnicode=true&characterEncoding=utf-8&useSSL=false"

  val MYSQL_USER = "debezium"

  val MYSQL_PASSWORD = "feng"

  //当前是否为测试环境
  var isTest = false

  val Phoenix_URL_LOCAL = "jdbc:phoenix:localhost:2181"

  val Phoenix_URL = "jdbc:phoenix:192.168.0.101:2181"

  val Phoenix_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver"

  val DEFAULT_INT = Integer.MIN_VALUE;

  val DEFAULT_LONG = Long.MinValue

  val DEFAULT_DOUBLE = Double.NaN

  val DEFAULT_STRING = ""

}
