package sample;

import redis.clients.jedis.JedisCluster;
import utils.RedisUtil;

import java.util.List;

/**
 * feng
 * 18-10-21
 */
public class RedisTest {

    public static void main(String[] args) {
        new RedisTest().redisTest();
    }

    public void redisTest() {
        JedisCluster jedis = RedisUtil.getJedisCluster();
        jedis.lpush("site-list", "Runoob");
        jedis.lpush("site-list", "Google");
        List list = jedis.lrange("site-list", 0, 1);
        System.out.println(list.toString());
        System.out.println(jedis.get("foo").toString());
    }
}
