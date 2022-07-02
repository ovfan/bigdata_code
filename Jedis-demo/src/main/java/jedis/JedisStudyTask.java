package jedis;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.List;

/**
 * @ClassName: JedisStudyTask
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年07月02日 17时26分
 * @Version: v1.0
 */
public class JedisStudyTask {
    public static void main(String[] args) {

    }

    /**
     * 创建jedis连接
     */
    public static JedisPool jedisPool = null;
    public static JedisPoolConfig jedisPoolConfig;
    public static Jedis jedis;
    @Before
    public void create(){
        if (jedisPool == null){
            // 连接池配置对象
            jedisPoolConfig = new JedisPoolConfig();
            // 1. 最大可用连接数
            jedisPoolConfig.setMaxTotal(20);
            // 最大闲置连接数
            jedisPoolConfig.setMaxIdle(6);
            // 最小闲置连接数
            jedisPoolConfig.setMinIdle(6);
            // 连接耗尽是否等待
            jedisPoolConfig.setBlockWhenExhausted(true);
            // 等待时长
            jedisPoolConfig.setMaxWaitMillis(2000L);
            // 取连接时候测试可用
            jedisPoolConfig.getTestOnBorrow();

            jedisPool = new JedisPool(jedisPoolConfig,"hadoop102",26379);
            jedis = jedisPool.getResource();
            jedis.auth("FanjingtaoRedis!@#8899");
        }
    }

    @Test
    public void test(){

        System.out.println(jedis.ping());
    }

    /**
     * TODO 测试string类型的方法
     */
    @Test
    public void testString() throws InterruptedException {
        // 1. 添加键值对数据
        jedis.select(1);
        jedis.set("k1","v1");
        jedis.set("k2","v2");
        // 1.1 当key不存在时添加数据
        Long setnx = jedis.setnx("k2", "v2hello");
        if (setnx == 0){
            System.out.println("添加失败");
        }
        // 1.2 添加数据的时候同时设置过期时间
        jedis.setex("k3",20,"v3");
        // 1.2.1 查看key的过期时间
        // Thread.sleep(2000L);
        Long ts = jedis.ttl("k3");
        System.out.println("还有" + ts + "s 数据过期");
        // 1.3 从指定位置开始覆盖旧值
        jedis.setrange("k2",2,"scala");
        System.out.println(jedis.get("k2"));    // v2hello -- > v2scala

        // 1.4 批量添加数据
        jedis.mset("k3","v3","k4","v4");
        // 1.4.1 当key不存在时批量添加数据
        jedis.msetnx("k4","v4","k5","v5");  // k4已经存在所以不会重复添加，导致整条语句添加失败
        jedis.msetnx("k5","v5","k6","v6");  // 这条添加成功

        // 2. 获取value
        String value = jedis.get("k2");
        System.out.println("k2 = " + value);
        // 2.1 批量获取数据
        List<String> result = jedis.mget("k1", "k2", "k3");
        System.out.println("result = " + result); // [v1, v2scala, v3]
        // 2.2 设置新值的同时获取旧值
        String oldValue = jedis.getSet("k2", "v2flink");
        System.out.println("k2的旧值为: " + oldValue + " ,新值为: " + jedis.get("k2"));

        //3. 在原有的值的基础上追加上内容
        jedis.append("k2","Top1");
        System.out.println(jedis.get("k2")); // v2flinkTop1

        // 4. 获取字符串长度
        Long length = jedis.strlen("k2");
        System.out.println("length = " + length);

        // 5. 获取value指定范围的子串数据 左闭右闭
        String K2ChildValue = jedis.getrange("k2", 2, 10);
        System.out.println("K2Value = " + K2ChildValue);    // flinkTop1

        // 6. value自增与自减
        jedis.set("k0","1");
        jedis.set("k7","2");
        jedis.incr("k0"); // k0自增1
        jedis.decr("k7");  // k7 自减 1  2-1 = 1
        // 6.1 带有步长的增减
        jedis.set("k8","10");
        jedis.incrBy("k8",5); // 10 + 5
        jedis.decrBy("k8",7);  // 15 -7 = 8
    }

    /**
     * TODO 测试list类型的方法
     */
    @Test
    public void testList(){

    }
    /**
     * TODO 测试set类型的方法
     */
    @Test
    public void testSet(){

    }
    /**
     * TODO 测试zset类型的方法
     */
    @Test
    public void testZset(){

    }

    /**
     * TODO 测试hash类型的方法
     */
    @Test
    public void testHash(){

    }
    @After
    public void close(){
        jedis.close();
    }
}
