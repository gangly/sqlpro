package com.pingan.pbear.util

import com.redis._
import org.slf4j.{Logger, LoggerFactory}


/**
  * Created by zhangrunqin on 16-11-20.
  */
object RedisUtil {
  private var redisConn: RedisClient = null
  private val log: Logger = LoggerFactory.getLogger(this.getClass)

  def init(host: String, port: Int, password: String): RedisClient = {
    if (null == redisConn)
      redisConn = new RedisClient(host, port, 0, Some(password))
    return redisConn
  }

  def del(key: String): Boolean = {
    try {
      redisConn.del(key)
      return true
    } catch {
      case e: Exception =>
        log.error(s"writing redis.${key} failed. msg: ${e.getMessage}, cause: ${e.getCause}, " +
          s"class: ${e.getClass}")
        throw new Exception(e)
    }
    return false
  }

  def set(key: String, any: Any): Boolean = {
    try {
      return redisConn.set(key, JsonUtil.toJson(any))
    } catch {
      case e: Exception =>
        log.error(s"writing redis.${key} failed. msg: ${e.getMessage}, cause: ${e.getCause}, " +
          s"class: ${e.getClass}")
        throw new Exception(e)
    }
    return false
  }

  def get[T: Manifest](key: String): Option[T] = {
    try {
      val json = redisConn.get[String](key).getOrElse("")
      if (!json.isEmpty)
        return Some(JsonUtil.fromJson[T](json))
      return None
    } catch {
      case e: Exception =>
        log.error(s"reading redis.${key} failed. msg: ${e.getMessage}, cause: ${e.getCause}, " +
          s"class: ${e.getClass}")
        throw new Exception(e)
    }
    return None
  }

  def main(args: Array[String]) {
    val conn = init("127.0.0.1", 6379, "123456")
    conn.set("RS_NEWS_test", "hello,world")
  }
}
