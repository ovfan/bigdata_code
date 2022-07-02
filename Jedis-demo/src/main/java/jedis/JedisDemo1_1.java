package jedis;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

/**
 * @ClassName: JedisDemo1_1
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年07月02日 17时03分
 * @Version: v1.0
 */
public class JedisDemo1_1 {
    public static void main(String[] args) {

    }
    public static Jedis jedis;
    // 获取jedis连接
    @Before
    public void getRedisConnection(){
        jedis = new Jedis("hadoop102",26379);
        // 密码验证
        jedis.auth("FanjingtaoRedis!@#8899");
    }

    @Test
    public void test(){
        // 测试连接
        String pong = jedis.ping();
        System.out.println("pong = " + pong);
    }

    // 关闭jedis连接
    @After
    public void close(){
        jedis.close();
    }
}
