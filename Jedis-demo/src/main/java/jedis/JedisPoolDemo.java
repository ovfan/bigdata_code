package jedis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @ClassName: JedisPoolDemo
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年07月02日 17时08分
 * @Version: v1.0
 */
public class JedisPoolDemo {

    public static void main(String[] args) {
        Jedis jedis = getJedisFromPool();
        jedis.auth("FanjingtaoRedis!@#8899");

        System.out.println(jedis.get("user:name"));
        // 将jedis连接还给连接池
        jedis.close();
    }
    // TODO 使用连接池技术管理jedis连接
    public static JedisPoolConfig jedisPoolConfig = null;
    public static Jedis getJedisFromPool() {

        // TODO 连接池配置对象相关配置
        if(jedisPoolConfig == null){
            jedisPoolConfig = new JedisPoolConfig();    // 连接池配置对象
        }
        jedisPoolConfig.setMaxTotal(20);    // 连接池最大可用连接数
        jedisPoolConfig.setMaxIdle(5);      // 最大闲置连接数
        jedisPoolConfig.setMinIdle(5);      // 最小闲置连接数
        jedisPoolConfig.setBlockWhenExhausted(true); //连接耗尽是否等待
        jedisPoolConfig.setMaxWaitMillis(2000L); // 最长等待时间为2s
        jedisPoolConfig.getTestOnBorrow();  //取连接的时候测试一下是否可用 ping pong

        // 获取连接池对象
        JedisPool jedisPool = new JedisPool(jedisPoolConfig, "hadoop102", 26379);

        // 返回jedis对象
        Jedis jedis = jedisPool.getResource();
        return jedis;
    }
}
