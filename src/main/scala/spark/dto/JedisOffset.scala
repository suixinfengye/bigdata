package spark.dto

import java.util

import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import utils.CommonUtil

import scala.collection.JavaConversions._

/**
  * feng
  */
object JedisOffset {
  val logger = LoggerFactory.getLogger(this.getClass)

  def apply(groupId: String) = {
    var fromdbOffset = Map[TopicPartition, Long]()
    val jedis = CommonUtil.getRedis
    //val s = RedisUtil.getLocalJedis
    val topicPartitionOffset: util.Map[String, String] = jedis.hgetAll(groupId)

    val topicPartitionOffsetlist: List[(String, String)] = topicPartitionOffset.toList
    logger.info("topicPartitionOffsetlist:" + topicPartitionOffsetlist.size)
    for (topicPL <- topicPartitionOffsetlist) {
      logger.info("topicPL.toString():"+topicPL.toString())
      logger.info("topicPL1.toString():"+topicPL._1.toString)
      val index = topicPL._1.lastIndexOf("-")
      fromdbOffset += (new TopicPartition(topicPL._1.substring(0,index), topicPL._1.substring(index+1).toInt) -> topicPL
        ._2.toLong)
    }
    logger.info("fromdbOffset:" + groupId + " " + fromdbOffset.toString())
    fromdbOffset
  }
}
