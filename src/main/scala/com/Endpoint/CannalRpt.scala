package com.Endpoint

import java.sql.{Connection, PreparedStatement}
import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import com.utils.{JdbcUtils, RptUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object CannalRpt {

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("目录参数不正确，退出程序")
      sys.exit()
    }
    val Array(inputPath) = args

    val ssc: SparkSession = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .enableHiveSupport()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val df = ssc.read.parquet(inputPath)
    val appInfoTup: RDD[(String, String)] = ssc.sparkContext.textFile("C:\\Users\\13996\\Desktop\\op\\app_dict.txt")
      .map(_.split("\\t"))
      .filter(_.length>=5)
      .map(arr => (arr(4), arr(1)))
    val map: Map[String, String] = appInfoTup.collect().toMap
    //广播app字典Map
    ssc.sparkContext.broadcast(map)

    import ssc.implicits._
    //数据进行处理
    val rdd: RDD[(String,String, List[Double])] = df.map(arr => {
      //取出需要的字段
      val reqm: Int = arr.getAs[Int]("requestmode")
      val proc: Int = arr.getAs[Int]("processnode")
      val iseff: Int = arr.getAs[Int]("iseffective")
      val isbil: Int = arr.getAs[Int]("isbilling")
      val isbid: Int = arr.getAs[Int]("isbid")
      val iswin: Int = arr.getAs[Int]("iswin")
      val ado: Int = arr.getAs[Int]("adorderid")
      val winpri: Double = arr.getAs[Double]("winprice")
      val adpay: Double = arr.getAs[Double]("adpayment")

      //key
      val appid: String = arr.getAs[String]("appid")
      val appname: String = arr.getAs[String]("appname")

      (appid,appname, RptUtils.request(reqm, proc) ++ RptUtils.click(reqm, iseff) ++ RptUtils.Ad(iseff, isbil, isbid, iswin, ado, winpri, adpay))
    }).rdd

    //先判定appname是否为空，若为空则用appid去字典找，
    val rddWithAppName: RDD[(String, List[Double])] = rdd
      .map(arr=>(if(arr._2.equals(" ")) map.getOrElse(arr._1,"unknow") else arr._2,arr._3))

    //val rddWithAppName: RDD[(String, List[Double])] = rdd.join(appInfoTup).map(arr=>(arr._2._2,arr._2._1))
    val ret: RDD[(String, List[Double])] = rddWithAppName.reduceByKey((a, b) => a.zip(b).map(tup => tup._1 + tup._2))
    val res: RDD[(String, Double, Double, Double, Double, Double, Double, Double, Double, Double)] = ret.map(a => (a._1, a._2(0), a._2(1), a._2(2), a._2(5), a._2(6), a._2(3), a._2(4), a._2(7),a._2(8)))

    res.foreachPartition(iter=>{
      //从连接池获取jdbc连接
      val conn: Connection = JdbcUtils.getConnection
      //遍历插入
      iter.foreach(tup=>{
        val ps: PreparedStatement = conn.prepareStatement("insert into cannalrpt1 values(?,?,?,?,?,?,?,?,?,?)")
        ps.setString(1,tup._1)
        ps.setDouble(2,tup._2)
        ps.setDouble(3,tup._3)
        ps.setDouble(4,tup._4)
        ps.setDouble(5,tup._5)
        ps.setDouble(6,tup._6)
        ps.setDouble(7,tup._7)
        ps.setDouble(8,tup._8)
        ps.setDouble(9,tup._9)
        ps.setDouble(10,tup._10)
        ps.execute()
      })
      //回收jdbc连接
      conn.close()
    })


//    res.toDF("渠道","总请求","有效请求","广告请求数","参与竞价数","竞价成功数","展示数","点击数","DSP广告消费","DSP广告成本")
//      .write.mode(SaveMode.Append)
//        .jdbc(getProperties()._2, "CannalRpt", getProperties()._1)
    //res.take(20).foreach(println)



  }
  def getProperties() = {
    val load: Config = ConfigFactory.load()
    val prop = new Properties()
    prop.setProperty("user",load.getString("jdbc.user"))
    prop.setProperty("password",load.getString("jdbc.password"))
    prop.setProperty("Driver",load.getString("jdbc.driver"))
    (prop, load.getString("jdbc.url"))
  }
  def getConn(): Unit ={

  }
}
