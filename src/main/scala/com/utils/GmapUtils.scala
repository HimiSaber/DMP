package com.utils

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}

import scala.collection.mutable.ListBuffer

/**
  * 商圈解析
  */
object GmapUtils {
  //获取搞得地图商圈信息
  def getBusinessFromGmap(long:Double,lat:Double):String ={

    val location: String = long+","+lat


    val urlStr :String = s"https://restapi.amap.com/v3/geocode/regeo?location=${location}&key=f8f10f998696bd5a13d7d86f216e89b1&radius=1000"

    val json: String = HttpUtils.get(urlStr)

    //解析Json串
    val jsonObj: JSONObject = JSON.parseObject(json)

    //判断状态
    if(jsonObj.getIntValue("status")==0) return ""

    //解析内部的Json，判断每个Key的value不为空

    val regeocodeJson = jsonObj.getJSONObject("regeocode")
    if(regeocodeJson == null || regeocodeJson.keySet().isEmpty) return ""

    val addressComponentJson = regeocodeJson.getJSONObject("addressComponent")
    if(addressComponentJson == null || addressComponentJson.keySet().isEmpty) return ""

    val arr = addressComponentJson.getJSONArray("businessAreas")
    if(arr == null || arr .isEmpty) return null


//    val arr: JSONArray = jsonObj.getJSONObject("regeocodes")
//
//      .getJSONObject("addressComponent")
//      .getJSONArray("businessAreas")

    //创建集合保存数据
    val buffer: ListBuffer[String] = ListBuffer[String]()
    //循环输出
    for(item <- arr.toArray()){
      if(item.isInstanceOf[JSONObject]){
        val js: JSONObject = item.asInstanceOf[JSONObject]
        buffer.append(js.getString("name"))
      }
    }
  buffer.mkString(",")
  }

}
