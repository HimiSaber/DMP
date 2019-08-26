package com.exam

import com.alibaba.fastjson.{JSON, JSONObject}

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks

object BusUtils {
  def getBusiness(json:String):ListBuffer[String] ={

    //解析Json串
    val jsonObj: JSONObject = JSON.parseObject(json)

    //判断状态
    if(jsonObj.getIntValue("status")==0) return null

    //解析内部的Json，判断每个Key的value不为空



    val regeocodeJson = jsonObj.getJSONObject("regeocode")
    if(regeocodeJson == null || regeocodeJson.keySet().isEmpty) return null

//    val addressComponentJson = regeocodeJson.getJSONObject("pois")
//    if(addressComponentJson == null || addressComponentJson.keySet().isEmpty) return null

    val arr = regeocodeJson.getJSONArray("pois")
    if(arr == null || arr .isEmpty) return null

    //创建集合保存数据
    val buffer: ListBuffer[String] = ListBuffer[String]()
    //循环输出
    for(item <- arr.toArray()){
      if(item.isInstanceOf[JSONObject]){
        val js: JSONObject = item.asInstanceOf[JSONObject]
        buffer.append(js.getString("businessarea"))
      }
    }
    buffer
  }

}
