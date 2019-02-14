package sample

import org.apache.kafka.common.TopicPartition
import redis.clients.jedis.Jedis
import spark.dto.JedisOffset
import utils.RedisUtil

/**
  * feng
  */
object RedisTestScala {

  def main(args: Array[String]): Unit = {
//    localRedisTest()
    clusterRedisTest()
//    val jedis  = CommonUtil.getRedis
//    val topicPartitionOffset: util.Map[String, String] = jedis.hgetAll("ProcessMysqlDataGroup")
//    println("------:"+topicPartitionOffset.toString)
  }

  def localRedisTest() = {
    val jedis: Jedis= RedisUtil.getLocalJedis
    println(jedis.ping)
    jedis.lpush("site-list", "Runoob")
    jedis.lpush("site-list", "Google")
    val list = jedis.lrange("site-list", 0, 1)
    println(list.toString)
  }

  def clusterRedisTest() = {
//    val jedis: JedisCluster = RedisUtil.getJedisCluster
    var formdbOffset: Map[TopicPartition, Long] = JedisOffset("ProcessMysqlDataGroup2")
    println("formdbOffset:"+formdbOffset)
    println("formdbOffset:"+formdbOffset.size)
//    jedis.lpush("site-list", "Runoob")
//    jedis.lpush("site-list", "Google")
//    val list = jedis.lrange("site-list", 0, 1)
//    println(list.toString)
  }
}
