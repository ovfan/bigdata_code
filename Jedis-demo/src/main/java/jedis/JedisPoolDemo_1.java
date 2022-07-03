package jedis;

import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.ListPosition;

import java.util.List;

/**
 * @ClassName: JedisPoolDemo_1
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年07月03日 09时24分
 * @Version: v1.0
 */
public class JedisPoolDemo_1 {

    @Test
    public void testList() {
        Jedis jedis = getJedisFromPool();
        jedis.select(1);
        jedis.flushDB();
        // 1. 添加数据
        jedis.lpush("l1", "v1", "v2", "v3", "v4");
        jedis.rpush("l5", "v5", "v6");

        // 2. 获取列表长度
        Long length = jedis.llen("l1");
        System.out.println("l1:length = " + length); // 4

        // 3. 查找数据
        // 3.1 根据索引查找数据
        String s = jedis.lindex("l1", 3);
        System.out.println("s = " + s); // 注意lpush从左边压入数据 ，所以要注意顺序问题 v1
        // 3.2 遍历指定范围的数据
        List<String> list = jedis.lrange("l1", 0, -1); // 全部取出
        // jedis.lrange("l1",0,jedis.llen("l1") -1);
        System.out.println("list = " + list); // list = [v4, v3, v2, v1]

        // 4. 插入数据
        jedis.linsert("l1", ListPosition.valueOf("AFTER"), "v4", "fanfan"); // 在值v4的后面插入fanfan
        // 注意：AFTER与BEFORE是枚举类型，这里要大写

        // 5. 删除数据
        // 5.1 从集合左边删除一个元素
        String readyPopVal = jedis.lindex("l1", 0);
        String popVal = jedis.lpop("l1");
        System.out.println(popVal.equals(readyPopVal));
        // 5.2 从集合右边删除一个元素
        // String rPopVal = jedis.rpop("l1");
        // System.out.println("rPopVal = " + rPopVal); // v1
        // 5.3 删除列表最后一个元素添加到另一个列表中的第一个位置
        jedis.rpoplpush("l1", "l5");
        List<String> l5 = jedis.lrange("l5", 0, -1);
        System.out.println("l5 = " + l5); // [v1, v5, v6] v1被插入到l5集合的第一个位置了

        // 5.4 从左边删除指定number 个 同一元素
        jedis.lpush("l2", "v1", "v1", "v3", "v1");
        jedis.lrem("l2", 3, "v1");//把3个 v1都删掉
        System.out.println(jedis.lrange("l2", 0, -1)); // [v3]
        // 5.5 保留指定索引范围的数据
        jedis.lpush("l3", "v1", "v1", "v3", "v1", "v4");
        jedis.ltrim("l3", 2, 3);
        System.out.println(jedis.lrange("l3", 0, -1)); // [v3, v1]
    }

    /**
     * TODO 从连接池中获取jedis
     *
     * @return
     */
    public static JedisPool jedisPool = null;

    public static Jedis getJedisFromPool() {
        if (jedisPool == null) {
            // 连接池相关配置
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
        // 验证密码
        jedis.auth("FanjingtaoRedis!@#8899");
        return jedis;
    }
}
