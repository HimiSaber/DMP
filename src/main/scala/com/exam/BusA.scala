package com.exam

import java.io.FileInputStream

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer
import scala.io.{BufferedSource, Source}
import scala.reflect.io.File

object BusA {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("目录参数不正确，退出程序")
      sys.exit()
    }
    //初始化
    val ssc: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName(this.getClass.getName)
      .getOrCreate()

    //参数
    val Array(inputPath) = args
    val txt: RDD[String] = ssc.sparkContext.textFile(inputPath)
    //拼接list
    var list: ListBuffer[String] = ListBuffer[String]()


    //用BusUtils工具类获取商圈列表
     txt.map(str => {
      val lis: ListBuffer[String] = BusUtils.getBusiness(str)
       lis
    }).collect().map(li=>list++:=li)

    //使用RDD算子聚合
    ssc.sparkContext.makeRDD(list)
      .map(str=>(str,1))
      .filter(_._1!="[]")
      .reduceByKey(_+_)
      .collect()
      .foreach(println)




  }
}
