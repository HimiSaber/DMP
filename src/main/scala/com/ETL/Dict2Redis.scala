package com.ETL

import com.utils.JedisUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import redis.clients.jedis.Jedis
import org.apache.http.ssl
object Dict2Redis {
  def main(args: Array[String]): Unit = {

    val ssc: SparkSession = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .enableHiveSupport()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val appInfoTup: RDD[(String, String)] = ssc.sparkContext.textFile("C:\\Users\\13996\\Desktop\\op\\app_dict.txt")
      .map(_.split("\\t"))
      .filter(_.length>=5)
      .map(arr => (arr(4), arr(1)))

    appInfoTup.foreachPartition(part=>{

        val jedis: Jedis = JedisUtils.getJedis()
      try {
        part.foreach(tup => jedis.set(tup._1, tup._2))
      }catch {
        case e:Exception => e.printStackTrace()
      }finally {
        if(jedis!=null){
          jedis.close()
        }
      }
    })

  }

}
