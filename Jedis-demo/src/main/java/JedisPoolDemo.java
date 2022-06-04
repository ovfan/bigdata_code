import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @ClassName: JedisDemo
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月04日 12时37分
 * @Version: v1.0
 *  连接池技术：降低开连接关连接 损耗
 *  连接池主要用来节省每次连接redis服务带来的连接消耗，将连接好的实例反复利用
 */
public class JedisPoolDemo {
    public static void main(String[] args) {
        Jedis jedis = getJedisFromPool();

        // 测试是否连通redis
        String result = jedis.ping();
        System.out.println(result);

        //
        jedis.set("user:name","taotao");
        String name = jedis.get("user:name");
        System.out.println("userName = " + name);

        // 关闭jedis连接对象---这一步是把连接对象还给连接池
        jedis.close();
    }

    /**
     * 连接池技术
     * @return jedis
     */
    public static JedisPool jedisPool = null;

    public static Jedis getJedisFromPool(){
        if(jedisPool == null){
            String host = "192.168.202.102";
            int port = 6379;

            // 连接池相关主要配置
            JedisPoolConfig jedisPoolConfig =new JedisPoolConfig();
            jedisPoolConfig.setMaxTotal(10); //最大可用连接数
            jedisPoolConfig.setMaxIdle(5); //最大闲置连接数
            jedisPoolConfig.setMinIdle(5); //最小闲置连接数
            jedisPoolConfig.setBlockWhenExhausted(true); //连接耗尽是否等待
            jedisPoolConfig.setMaxWaitMillis(2000); //等待时间
            jedisPoolConfig.setTestOnBorrow(true); //取连接的时候进行一下测试 ping pong

            jedisPool = new JedisPool(jedisPoolConfig,host,port);
        }
        // 从池中获取Jedis对象
        Jedis jedis = jedisPool.getResource();
        return jedis;
    }
}
