package jedis;

import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @ClassName: JedisPoolDemo_2
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年07月04日 14时15分
 * @Version: v1.0
 */
public class JedisPoolDemo_2 {
    public static JedisPool jedisPool = null;
    public static Jedis getJedisFromPoll(){
        if(jedisPool == null){

            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
            jedisPoolConfig.setMaxTotal(20);
            jedisPoolConfig.setMaxIdle(5);
            jedisPoolConfig.setMinIdle(5);
            jedisPoolConfig.setBlockWhenExhausted(true);
            jedisPoolConfig.setMaxWaitMillis(2000L);
            jedisPoolConfig.getTestOnBorrow();

            jedisPool = new JedisPool(jedisPoolConfig,"hadoop102",26379);

        }
        Jedis jedis = jedisPool.getResource();
        jedis.auth("FanjingtaoRedis!@#8899");
        return  jedis;
    }
    @Test
    public void test(){
        Jedis jedis = getJedisFromPoll();
        String pong = jedis.ping();
        System.out.println("pong = " + pong);
    }
}
