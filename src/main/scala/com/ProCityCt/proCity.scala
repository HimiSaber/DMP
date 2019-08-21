package com.ProCityCt

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object proCity {

  def main(args: Array[String]): Unit = {
    if(args.length != 2){
      println("目录参数不正确，退出程序")
      sys.exit()
    }

    val Array(inputPath,outputPath) = args
    val ssc: SparkSession = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .enableHiveSupport()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val df: DataFrame = ssc.read.parquet(inputPath)
    df.createOrReplaceTempView("t_log")
    val ret: DataFrame = ssc.sql("select provincename,cityname,count(1) ct from t_log group by provincename,cityname")

//    ret.coalesce(1)
//      .write
//      .partitionBy("provincename","cityname")
//      .format("json")
//      .save(args(1))
    ret.write.mode(SaveMode.Append)
      .jdbc(getProperties()._2,"cityCnt",getProperties()
    ._1)






//    val pc: DataFrame = df.select("provincename","cityname")
//    val ret: RDD[((String, String), Int)] = pc.rdd.map(arr=>((arr(0).toString,arr(1).toString),1)).reduceByKey(_+_)
//
//    val res: RDD[(String, String, Int)] = ret.map(a=>(a._1._1,a._1._2,a._2))
//
//    import ssc.implicits._
//    //    res.toDF("provincename","cityname","ct")
//    //      .write
//    //      .mode(SaveMode.Append)
//    //      .jdbc(getProperties()._2,"cityCntWithProv",getProperties()._1)
//
//    val dfr: DataFrame = res.toDF("provincename","cityname","ct")
//
//    dfr.write.partitionBy("provincename","cityname").format("json").save(args(1))


  }



  def getProperties() = {
    val load: Config = ConfigFactory.load()
    val prop = new Properties()
    prop.setProperty("user",load.getString("jdbc.user"))
    prop.setProperty("password",load.getString("jdbc.password"))
    prop.setProperty("Driver",load.getString("jdbc.driver"))
    (prop, load.getString("jdbc.url"))
  }


}
