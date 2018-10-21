package sample

import java.util

import redis.clients.jedis.{HostAndPort, Jedis, JedisCluster, JedisPoolConfig}
import utils.RedisUtil

/**
  * feng
  * 18-10-16
  */
object RedisTestScala {

  def main(args: Array[String]): Unit = {
//    localRedisTest()
    clusterRedisTest()
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
    val jedis: JedisCluster = RedisUtil.getJedisCluster
    jedis.lpush("site-list", "Runoob")
    jedis.lpush("site-list", "Google")
    val list = jedis.lrange("site-list", 0, 1)
    println(list.toString)
  }
}
