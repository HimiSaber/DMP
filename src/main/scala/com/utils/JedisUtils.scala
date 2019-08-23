package com.utils

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object JedisUtils {
  private val conf:JedisPoolConfig = new JedisPoolConfig()

  private def init(): Unit ={
    conf.setMaxTotal(30)
    conf.setMaxIdle(10)
  }

  private val pool = new JedisPool(conf,"192.168.126.132",6379)


  def getJedis(): Jedis={
    pool.getResource
  }




}
