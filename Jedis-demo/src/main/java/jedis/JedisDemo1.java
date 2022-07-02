package jedis;

import redis.clients.jedis.Jedis;

/**
 * @ClassName: JedisDemo1
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年07月02日 16时25分
 * @Version: v1.0
 * TODO 复习jedis
 */
public class JedisDemo1 {
    public static void main(String[] args) {
        Jedis jedis = getJedis();
        // 如果设置了redis密码，需要在jedis对象中配置
        jedis.auth("FanjingtaoRedis!@#8899");

        // 测试redis是否连通
        String result = jedis.ping();
        System.out.println("result = " + result);

        // 设置一个k v 键值对数据
        jedis.set("user:name","atguigu");
        jedis.close();
    }

    public static Jedis getJedis() {
        String host = "hadoop102";
        int port = 26379;
        Jedis jedis = new Jedis(host, port);
        return jedis;
    }
}
