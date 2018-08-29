package sample
//import org.apache.spark._
//import org.apache.spark.rdd.NewHadoopRDD
//import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor}
//import org.apache.hadoop.hbase.client.HBaseAdmin
//import org.apache.hadoop.hbase.mapreduce.TableInputFormat
//import org.apache.hadoop.fs.Path
//import org.apache.hadoop.hbase.HColumnDescriptor
//import org.apache.hadoop.hbase.util.Bytes
//import org.apache.hadoop.hbase.client.Put
//import org.apache.hadoop.hbase.client.HTable
//
////./spark/bin/spark-shell --master spark://spark1:7077 --executor-memory 600m  --executor-cores 1
//
////https://docs.microsoft.com/en-us/azure/hdinsight/hdinsight-using-spark-query-hbase
//import org.apache.spark.sql.SparkSession;
object HbaseSpark {
//  val spark = SparkSession
//    .builder()
//    //      .master("local")
//    .appName("HbaseSpark")
//    .getOrCreate()
//
//  val conf = HBaseConfiguration.create()
//  conf.set("hbase.zookeeper.property.clientPort", "2181"); //设置zookeeper client端口
//  conf.set("hbase.zookeeper.quorum","spark1,spark2,spark3")
//  conf.addResource("/usr/local/userlib/hbase/conf/hbase-site.xml")  //将hbase的配置加载
//  conf.set(org.apache.hadoop.hbase.mapreduce.TableInputFormat.INPUT_TABLE,"TEST_PERSON")
//  val hbaseRDD = spark.sparkContext.newAPIHadoopRDD(conf, classOf[org.apache.hadoop.hbase.mapreduce.TableInputFormat], classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable], classOf[org.apache.hadoop.hbase.client.Result])
//  hbaseRDD.count()

}
