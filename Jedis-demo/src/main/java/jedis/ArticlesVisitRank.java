package jedis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Set;

/**
 * @ClassName: ArticlesVisitRank
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年07月02日 18时11分
 * @Version: v1.0
 *
 */
public class ArticlesVisitRank {
    /**
     * TODO 使用redis完成文章访问量排行的功能
     * 使用Redis存数据的时候必须考虑5个问题
     * 1. 使用什么数据类型存
     * 2. key的设计
     * 3. value的设计
     * 4. 写入使用什么命令
     * 5. 查询使用什么命令
     *
     * @param args zset article:visit:rank
     */
    public static void main(String[] args) {
        // 获取jedis对象
        Jedis jedis = getJedisFromPool();
        jedis.auth("FanjingtaoRedis!@#8899");
        // TODO test看一下zset存储的结构
        jedis.select(2);
        // jedis.zadd("k1",50,"v1");
        // jedis.zadd("k1",60,"v2");
        // Set<String> set = jedis.zrange("k1", 0, -1);
        // System.out.println("set = " + set); // set = [v1, v2]

        // 1.使用zset类型来存文章访问量排行
        // 2. key与value的设计 article:visit:rank [scala,java,flink,spark,redis,flume]
        // 3. 写入操作
        // TODO 初始点击
        jedis.zadd("article:visit:rank", 10, "java");
        jedis.zadd("article:visit:rank", 10, "scala");
        jedis.zadd("article:visit:rank", 10, "flink");
        jedis.zadd("article:visit:rank", 10, "spark");
        jedis.zadd("article:visit:rank", 10, "redis");
        jedis.zadd("article:visit:rank", 10, "flume");

        // 模拟点击操作
        for (int i = 1; i <= 10; i++) {
            if (i <= 4) {
                jedis.zincrby("article:visit:rank", 10, "java");//点一次，评分 + 10
            }
            if (i <= 3) {
                jedis.zincrby("article:visit:rank", 10, "scala");//点一次，评分 + 10
            }
            if (i <= 7) {
                jedis.zincrby("article:visit:rank", 10, "flink");//点一次，评分 + 10
            }
            if (i <= 6) {
                jedis.zincrby("article:visit:rank", 10, "spark");//点一次，评分 + 10
            }
            if (i <= 5) {
                jedis.zincrby("article:visit:rank", 10, "redis");//点一次，评分 + 10
            }
            if (i <= 10) {
                jedis.zincrby("article:visit:rank", 10, "flume");//点一次，评分 + 10
            }
        }
        // 获取TOPN排行
        Set<String> zrevrange = jedis.zrevrange("article:visit:rank", 0, -1);
        System.out.println("zrevrange = " + zrevrange);

        // 获取评分
        Double flinkScore = jedis.zscore("article:visit:rank", "flink");
        System.out.println("flinkScore = " + flinkScore);


        jedis.close();
    }

    public static JedisPool jedisPool = null;

    public static Jedis getJedisFromPool() {
        if (jedisPool == null) {
            //连接池相关配置
            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
            jedisPoolConfig.setMaxTotal(20);
            jedisPoolConfig.setMaxIdle(5);
            jedisPoolConfig.setMinIdle(5);
            jedisPoolConfig.setBlockWhenExhausted(true);
            jedisPoolConfig.setMaxWaitMillis(2000L);
            jedisPoolConfig.getTestOnBorrow();

            jedisPool = new JedisPool(jedisPoolConfig, "hadoop102", 26379);
        }
        Jedis jedis = jedisPool.getResource();
        return jedis;
    }

}
