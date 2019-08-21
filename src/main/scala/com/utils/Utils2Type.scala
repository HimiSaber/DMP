package com.utils

/**
  * 数据类型转换
  *
  */
object Utils2Type {
  //String 转换成Int
  def toInt(str: String): Int ={
    try {
      str.toInt
    }catch{
      case _ : Exception => 0
    }
  }


  //String 转换Double

  def toDouble(str:String):Double ={
    try {
      str.toDouble
    }catch{
      case _ : Exception => 0.0
    }
  }
}
