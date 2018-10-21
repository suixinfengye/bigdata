package spark.dto

import java.util

import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import utils.RedisUtil
import scala.collection.JavaConversions._

/**
  * feng
  * 18-10-21
  */
object JedisOffset {
  val logger = LoggerFactory.getLogger(this.getClass)

  def apply(groupId: String) = {
    var fromdbOffset = Map[TopicPartition, Long]()
    val jedis1 = RedisUtil.getJedisCluster
    val topicPartitionOffset: util.Map[String, String] = jedis1.hgetAll(groupId)

    val topicPartitionOffsetlist: List[(String, String)] = topicPartitionOffset.toList
    for (topicPL <- topicPartitionOffsetlist) {
      val split: Array[String] = topicPL._1.split("[-]")
      fromdbOffset += (new TopicPartition(split(0), split(1).toInt) -> topicPL._2.toLong)
    }
    if (fromdbOffset.isEmpty) {
      (fromdbOffset.toMap, 0)
    } else {
      (fromdbOffset.toMap, 1)
    }
    logger.info("fromdbOffset:" + groupId + " " + fromdbOffset.toString)
    fromdbOffset
  }
}
