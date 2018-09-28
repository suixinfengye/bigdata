package sample

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
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
//https://www.jianshu.com/p/0db275b06496
object HbaseSpark extends Logging {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      //      .master("local")
      .appName("HbaseSpark")
      .getOrCreate()

    val tableName = "test"
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181"); //设置zookeeper client端口
    conf.set("hbase.zookeeper.quorum", "spark1,spark2,spark3")
    //    conf.addResource("/usr/local/userlib/hbase/conf/hbase-site.xml") //将hbase的配置加载
    conf.set(org.apache.hadoop.hbase.mapreduce.TableInputFormat.INPUT_TABLE, tableName)

    readHbase(spark,conf)
    writeHbase(spark,conf)
  }

  def readHbase(spark: SparkSession,conf: Configuration): Unit = {
    val hbaseRDD = spark.sparkContext.newAPIHadoopRDD(conf, classOf[org.apache.hadoop.hbase.mapreduce.TableInputFormat], classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable], classOf[org.apache.hadoop.hbase.client.Result])
    val value: Long = hbaseRDD.count()
    logInfo("---------------------------============================----------------------------------: " + value)
//    /
  }

  def writeHbase(spark:SparkSession,conf: Configuration):Unit ={
    val jobConf = new JobConf(conf,this.getClass)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,"test")
    val rawData = List(("03","info","name","haha"))
    val localData = spark.sparkContext.parallelize(rawData).map(t=>converToPut(HbaseCell(t._1,t._2,t._3,t._4)))
    localData.saveAsHadoopDataset(jobConf)
  }

  def converToPut(c:HbaseCell) ={
    val put = new Put(Bytes.toBytes(c.rowKey))
    put.addColumn(Bytes.toBytes(c.cf),Bytes.toBytes(c.qf),Bytes.toBytes(c.value))
    (new ImmutableBytesWritable, put)
  }

}
case class HbaseCell(rowKey:String,cf:String,qf:String,value:String)
