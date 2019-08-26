package com.Tag

import com.utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object TagsKey extends Tag{
  /**
    *
    * 打标签统一接口
    *
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = ListBuffer[(String,Int)]()

    //解析参数
    val row: Row = args(0).asInstanceOf[Row]
    val stopword = args(1).asInstanceOf[Broadcast[Map[String, String]]]

    //关键字tag
    val keywords: String = row.getAs[String]("keywords")
    keywords.split("\\|")
      .filter(word=>{
        word.length>=3 && word.length <=8 && !stopword.value.contains(word)
      })
      .foreach(str=>{
        list .append( ("K"+str,1))
      })

    list.toList
  }
}
