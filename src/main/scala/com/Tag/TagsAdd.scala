package com.Tag

import com.utils.{JedisUtils, Tag}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object TagsAdd extends Tag{
  /**
    *
    * 打标签统一接口
    *
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = ListBuffer[(String,Int)]()

    //解析参数
    val row: Row = args(0).asInstanceOf[Row]
    //解析广播变量
    //val appInfo: Map[String, String] = args(1).asInstanceOf[Map[String, String]]

    //解析Jedis连接
    val jedis: Jedis = args(1).asInstanceOf[Jedis]

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


    //获取App名称
    val appname: String = row.getAs[String]("appname")
    if(appname.equals(" ")) {
      //list.append(("APP" + appInfo.getOrElse(row.getAs[String]("appid"), "未知"), 1))
      list.append(("APP"+jedis.get(row.getAs[String]("appid")),1))
    }else{
      list.append(("APP"+appname,1))
    }





    //渠道标签
    val cannalId: Int = row.getAs[Int]("adplatformproviderid")
    list .append( ("CN"+cannalId,1))

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

    //关键字tag
    val keywords: String = row.getAs[String]("keywords")
    keywords.split("\\|")
      .filter(str=>str.length>2&&str.length<9)
      .foreach(str=>{
        list .append( ("K"+str,1))
      })


    //地域tag
    val provincename: String = row.getAs[String]("provincename")
    list .append( ("ZP"+provincename,1))
    val cityname: String = row.getAs[String]("cityname")
    list .append( ("ZC"+cityname,1))

      list.toList
  }
}
