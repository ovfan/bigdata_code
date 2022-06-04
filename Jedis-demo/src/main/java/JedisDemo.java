import redis.clients.jedis.Jedis;

/**
 * @ClassName: JedisDemo
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月04日 12时37分
 * @Version: v1.0
 * 测试Jedis
 *  Jedis中的方法都是 redis中的方法，记住redis中的API操作很重要
 */
public class JedisDemo {
    public static void main(String[] args) {
        Jedis jedis = getJedis();

        // 测试是否连通redis
        String result = jedis.ping();
        System.out.println(result);

        //
        jedis.set("user:name","fanfan");
        String name = jedis.get("user:name");
        System.out.println("userName = " + name);

        // 关闭jedis连接对象
        jedis.close();
    }


    public static Jedis getJedis() {
        String host = "192.168.202.102";
        int port = 6379;
        Jedis jedis = new Jedis(host, port);
        return jedis;
    }
}
