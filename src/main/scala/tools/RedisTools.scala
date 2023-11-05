package tools

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
 * @author: BYDylan
 * @date: 2023/3/2
 * @description: Redis工具类
 */
object RedisTools {
  var jedisPool: JedisPool = null

  def getJedisFromPool(): Jedis = {
    if (jedisPool == null) {
      // 创建连接池对象
      // 连接池配置
      val jedisPoolConfig = new JedisPoolConfig()
      jedisPoolConfig.setMaxTotal(100) // 最大连接数
      jedisPoolConfig.setMaxIdle(20) // 最大空闲
      jedisPoolConfig.setMinIdle(20) // 最小空闲
      jedisPoolConfig.setBlockWhenExhausted(true) // 忙碌时是否等待
      jedisPoolConfig.setMaxWaitMillis(5000) // 忙碌时等待时长 毫秒
      jedisPoolConfig.setTestOnBorrow(true) // 每次获得连接的进行测试
      val host: String = PropsTools(Config.REDIS_HOST)
      val port: String = PropsTools(Config.REDIS_PORT)

      jedisPool = new JedisPool(jedisPoolConfig, host, port.toInt)
    }
    jedisPool.getResource
  }
}
