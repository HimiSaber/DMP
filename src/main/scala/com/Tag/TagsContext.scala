package com.Tag

import com.utils.{JedisUtils, TagUtils}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

object TagsContext {
  def main(args: Array[String]): Unit = {
    if(args.length != 4) {
      println("目录不匹配，退出程序")
      sys.exit()
    }

    val Array(inputPath,outputPath,appinfoPath,unPath)=args


    //创建上下文
//    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
//    new SparkContext(conf)
    val ssc: SparkSession = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

//    val appInfoTup: RDD[(String, String)] = ssc.sparkContext.textFile(appinfoPath)
//      .map(_.split("\\t"))
//      .filter(_.length>=5)
//      .map(arr => (arr(4), arr(1)))
//    val map: Map[String, String] = appInfoTup.collect().toMap
//    //广播app字典Map
//    val boradcast: Broadcast[Map[String, String]] = ssc.sparkContext.broadcast(map)




    //读取数据
//    val df: DataFrame = ssc.read.parquet(inputPath)
//    val tags: RDD[(String, List[(String, Int)])] = df.filter(TagUtils.OneUserId)
//      .rdd.map(row => {
//      //接下来所有的标签都在内部实现
//      //取出用户id(key)
//      val userId: String = TagUtils.getOneUserId(row)
//      //通过Row数据打标签
//      val adList: List[(String, Int)] = TagsAdd.makeTags(row, boradcast.value)
//      //val adList: List[(String, Int)] = TagsAdd.makeTags(row)
//      (userId, adList)
//    })

    //redis版本
    val df: DataFrame = ssc.read.parquet(inputPath)
    val tags: RDD[(String, List[(String, Int)])] = df.filter(TagUtils.OneUserId)
      .rdd.mapPartitions(part=>{
      val jedis: Jedis = JedisUtils.getJedis()
      try {
        part.map(row => {
          //接下来所有的标签都在内部实现
          //取出用户id(key)
          val userId: String = TagUtils.getOneUserId(row)
          //通过Row数据打标签
          //val adList: List[(String, Int)] = TagsAdd.makeTags(row, boradcast.value)
          val adList: List[(String, Int)] = TagsAdd.makeTags(row,jedis)
          (userId, adList)
        })
      }finally {
        jedis.close()
      }
    })





    tags.saveAsTextFile(outputPath)




  }

}
