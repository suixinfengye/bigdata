package sample

import redis.clients.jedis.Jedis

/**
  * feng
  * 18-10-16
  */
object RedisTest {
  def main(args: Array[String]): Unit = {
    val jedis: Jedis = new Jedis("localhost")
    println(jedis.ping())
    getSet(jedis)
  }

  def getSet(jedis: Jedis) = {
    jedis.lpush("site-list", "Runoob")
    jedis.lpush("site-list", "Google")
    val list = jedis.lrange("site-list", 0, 1)
    println(list.toString)
  }
}
