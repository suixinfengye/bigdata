package sample

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.StatCounter

//
////./spark/bin/spark-shell --master spark://spark1:7077 --executor-memory 600m  --executor-cores 1
//
////https://docs.microsoft.com/en-us/azure/hdinsight/hdinsight-using-spark-query-hbase
//./bin/spark-submit \
//--class sample.HbaseSpark \
//--master spark://spark1:7077 \
//--executor-memory 700m \
//--total-executor-cores 2 \
///usr/local/userlib/jars/bigdata.jar

object HbaseSpark extends Logging {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      //      .master("local")
      .appName("HbaseSpark")
      .getOrCreate()

  }

  def readHbase(spark: SparkSession): Unit = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181"); //设置zookeeper client端口
    conf.set("hbase.zookeeper.quorum", "spark1,spark2,spark3")
    conf.addResource("/usr/local/userlib/hbase/conf/hbase-site.xml") //将hbase的配置加载
    conf.set(org.apache.hadoop.hbase.mapreduce.TableInputFormat.INPUT_TABLE, "test")
    val hbaseRDD = spark.sparkContext.newAPIHadoopRDD(conf, classOf[org.apache.hadoop.hbase.mapreduce.TableInputFormat], classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable], classOf[org.apache.hadoop.hbase.client.Result])
    val value: Long = hbaseRDD.count()
    logInfo("---------------------------============================----------------------------------: " + value)
    logError("---------------------------============================----------------------------------: " + value)
  }

  def writeHbase(spark:SparkSession):Unit ={
    val conf = HBaseConfiguration.create()
    val jobConfig: JobConf = new JobConf(conf, this.getClass)
    jobConfig.set("mapreduce.output.fileoutputformat.outputdir", "/user/user01/out")
//    jobConfig.setOutputFormat(classOf[TableOutputFormat])
    jobConfig.set(TableOutputFormat.OUTPUT_TABLE, "test")
    // convert rowkey, psi stats to put and write to hbase table stats column family
//    keyStatsRDD.map { case (k, v) => convertToPut(k, v) }.saveAsHadoopDataset(jobConfig)
    convertToPut().save
  }
  // convert rowkey, stats to put
  def convertToPut(): (ImmutableBytesWritable, Put) = {
    val p = new Put(Bytes.toBytes("key"))
    // add columns with data values to put
    p.add(Bytes.toBytes("cloumn famliy"), Bytes.toBytes("cloumn qulify"), Bytes.toBytes("dddd"))
    (new ImmutableBytesWritable, p)
  }
}
