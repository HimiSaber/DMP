package com.Tag

import com.utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object TagsApp extends Tag{
  /**
    *
    * 打标签统一接口
    *
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = ListBuffer[(String,Int)]()

    //解析参数
    val row: Row = args(0).asInstanceOf[Row]
    //解析Jedis连接
    val jedis: Jedis = args(1).asInstanceOf[Jedis]


    //获取App名称
    val appname: String = row.getAs[String]("appname")
    if(appname.equals(" ")) {
      list.append(("APP"+jedis.get(row.getAs[String]("appid")),1))
    }else{
      list.append(("APP"+appname,1))
    }

    list.toList
  }
}
