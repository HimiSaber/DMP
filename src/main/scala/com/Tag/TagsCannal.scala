package com.Tag

import com.utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object TagsCannal extends Tag{
  /**
    *
    * 打标签统一接口
    *
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = ListBuffer[(String,Int)]()

    //解析参数
    val row: Row = args(0).asInstanceOf[Row]
    //渠道标签
    val cannalId: Int = row.getAs[Int]("adplatformproviderid")
    list .append( ("CN"+cannalId,1))

    list.toList
  }
}
