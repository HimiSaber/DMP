package com.exam

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

object TypeTag {
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

    var list: ListBuffer[String] = ListBuffer[String]()


    //用BusUtils工具类获取商圈列表
    txt.map(str => {
      val lis: ListBuffer[String] = TypeUtils.getType(str)
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
