package com.Tag

import ch.hsr.geohash.GeoHash
import com.utils.{GmapUtils, Tag, Utils2Type}
import org.apache.spark.sql.Row
import org.apache.commons.lang3.StringUtils
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/**
  * 商圈解析：
  * 获取商圈将信息 存入redis
  * key使用GeoHash处理的经纬度
  */

object TagBusiness extends Tag{
  /**
    *
    * 打标签统一接口
    *
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    val list: ListBuffer[(String, Int)] = ListBuffer[(String,Int)]()

    //解析参数
    val row: Row = args(0).asInstanceOf[Row]
    val jedis: Jedis = args(1).asInstanceOf[Jedis]
    val long: Double = Utils2Type.toDouble(row.getAs[String]("long"))
    val lat: Double = Utils2Type.toDouble(row.getAs[String]("lat"))


    //获取经纬度
    if(long>=73.0 &&
      long.toDouble<=135.0 &&
      lat.toDouble>=3.0 &&
      lat.toDouble<=54.0
    ){



      val bussiness: String = getBus(long,lat,jedis)

      bussiness.split(",").map(str=>list.append((str,1)))


    }
    list.toList
  }




  //获取商圈信息
  def getBus(long:Double,lat:Double,j: Jedis):String ={
    //转换经纬度字符串使用GeoHash
    val str: String = GeoHash.geoHashStringWithCharacterPrecision(lat,long,6)
    //去获取商圈信息
    val bus: String = redisQuery(str,j)
    if (StringUtils.isNotBlank(bus)) {
      bus
    }else{
      val bus: String = GmapUtils.getBusinessFromGmap(long,lat)
      redisSave(str,bus,j)
      bus
    }

  }
  //
  def redisQuery(geo:String,j:Jedis):String={
    val str: String = j.get(geo)
    str
  }

  def redisSave(geo:String,bus:String,jedis: Jedis){
    jedis.set(geo,bus)
  }
}
