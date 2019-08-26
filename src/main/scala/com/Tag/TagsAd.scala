package com.Tag

import com.utils.{JedisUtils, Tag}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object TagsAd extends Tag{
  /**
    *
    * 打标签统一接口
    *
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = ListBuffer[(String,Int)]()

    //解析参数
    val row: Row = args(0).asInstanceOf[Row]

    //获取广告类型、广告类型名称
    val adType: Int = row.getAs[Int]("adspacetype")
    adType match {
      case v if v>9 => list.append(("LC"+v,1))
      case v if v<=9 && v>=0 => list.append(("LC0"+v,1))
    }

    val adName: String = row.getAs[String]("adspacetypename")
    if(StringUtils.isNotBlank(adName)){
      list .append( ("LN"+adName,1))
    }

      list.toList
  }
}
