package spark.analysis

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}

/**
  * 给电影评论做聚合,将多条评论聚合成一个string
  * feng
  * 19-1-19
  */
class AggComments extends UserDefinedAggregateFunction {

  // inputSchema，输入数据的类型
  def inputSchema: StructType = {
    StructType(Array(StructField("COMMENT", StringType, true)))
  }

  // bufferSchema，中间进行聚合时，所处理的数据的类型
  def bufferSchema: StructType = {
    StructType(Array(StructField("content", StringType, true)))
  }

  // dataType，函数返回值的类型
  def dataType: DataType = {
    StringType
  }

  //Deterministic just means that for the same input the function will return the same output.
  //Unless there is some randomness in your function it'll always be deterministic.
  def deterministic: Boolean = {
    true
  }

  // 为每个分组的数据执行初始化操作
  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = ""
  }

  // 由于Spark是分布式的，所以一个分组的数据，可能会在不同的节点上进行局部聚合，就是update
  // 每个分组，有新的值进来的时候，如何进行分组对应的聚合值的计算
  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[String](0) + input.getAs[String](0)
  }

  // 最后一个分组，在各个节点上的聚合值，要进行merge，也就是合并
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[String](0) + buffer2.getAs[String](0)
  }

  // 返回一个最终的聚合值
  def evaluate(buffer: Row): Any = {
    buffer.getAs[String](0)
  }

}
