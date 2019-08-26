package com.Tag

import com.utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object TagsDevice extends Tag{
  /**
    *
    * 打标签统一接口
    *
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = ListBuffer[(String,Int)]()

    //解析参数
    val row: Row = args(0).asInstanceOf[Row]

    //设备
    val client: Int = row.getAs[Int]("client")
    client match {
      case v if v == 1 => list .append( ("Android D00010001",1))
      case v if v == 2 => list .append( ("IOS D00010002",1))
      case v if v == 3 => list .append( ("WinPhone D00010003",1))
      case _ => list .append( ("其他 D00010004",1))
    }
    val network: Int = row.getAs[Int]("networkmannerid")
    network match {
      case v if v == 1 => list .append( ("WIFI D00020001",1))
      case v if v == 2 => list .append( ("4G D00020002",1))
      case v if v == 3 => list .append( ("3G D00020003",1))
      case v if v == 4 => list .append( ("2G D00020004",1))
      case _ => list .append( ("D00020005",1))
    }
    val ispid: Int = row.getAs[Int]("ispid")
    ispid match  {
      case v if v == 1 => list .append( ("移 动 D00030001",1))
      case v if v == 2 => list .append( ("联 通 D00030002 ",1))
      case v if v == 3 => list .append( ("电 信 D00030003",1))
      case _ => list .append( ("D00030004",1))
    }



    list.toList
  }
}
