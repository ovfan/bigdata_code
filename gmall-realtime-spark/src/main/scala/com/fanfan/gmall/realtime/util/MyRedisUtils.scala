package com.fanfan.gmall.realtime.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
 * redis工具类，用于获取 Jedis对象
 */
object MyRedisUtils {
  // 获取连接池对象
  var jedisPool: JedisPool = null

  // 获取jedis对象
  def getJedis(): Jedis = {
    if (jedisPool == null) {
      // redis连接相关参数
      val host: String = MyPropsUtils(MyConfigUtils.REDIS_HOST)
      val port: String = MyPropsUtils(MyConfigUtils.REDIS_PORT)
      val passwd: String = MyPropsUtils(MyConfigUtils.REDIS_AUTH_PASSWORD)

      val jedisPoolConfig = new JedisPoolConfig()
      // 连接池相关配置
      jedisPoolConfig.setMaxTotal(100) //最大连接数
      jedisPoolConfig.setMaxIdle(20) //最大空闲
      jedisPoolConfig.setMinIdle(20) //最小空闲
      jedisPoolConfig.setBlockWhenExhausted(true) //忙碌时是否等待
      jedisPoolConfig.setMaxWaitMillis(5000) //忙碌时等待时长 毫秒
      jedisPoolConfig.setTestOnBorrow(true) //每次获得连接的进行测试

      // 为redis设置密码 (需要设置 连接超时时间 + 密码 )
      jedisPool = new JedisPool(jedisPoolConfig, host, port.toInt, 3000, passwd)

    }
    jedisPool.getResource
  }

  // 关闭jedis连接
  def close(jedis: Jedis): Unit = {
    jedis.close()
  }

  // 测试jedis是否正常连接
  def main(args: Array[String]): Unit = {
    val jedis: Jedis = getJedis()
    println(jedis.ping())

    //关闭连接
    close(jedis)
  }
}
