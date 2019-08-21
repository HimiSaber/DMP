package com.Rpt

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import com.utils.RptUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object DeviceRpt {
  def main(args: Array[String]): Unit = {
    if(args.length != 1){
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
    /**
      * Sql版本
      */
//    df.createOrReplaceTempView("t_log")
//
//    val ret: DataFrame = ssc.sql("select  case devicetype when 1 then '手机'  when 2 then '平板'  else '其他' end `设备类型`," +
//      "sum(originReq) `原始请求数`,sum(efftReq) `有效请求数`,sum(adReq) `广告请求数`,sum(partBid) `参与竞价数`," +
//      "sum(bidWin) `竞价成功数`, sum(showNum) `展示数`, sum(clickNum) `点击数`, sum(dsppay)/1000 `DSP广告消费`,sum(dscost)/1000 `DSP广告成本` " +
//      " from (select devicetype, " +
//      " case when requestmode=1 and processnode>=1 then 1 else 0 end originReq, " +
//      "case when requestmode=1 and processnode>=2 then 1 else 0 end efftReq, " +
//      "case when requestmode=1 and processnode>=3 then 1 else 0 end adReq," +
//      "case when iseffective=1 and isbilling=1 and isbid=1 then 1 else 0 end partBid," +
//      "case when iseffective=1 and isbilling=1 and iswin=1 and adorderid<>0 then 1 else 0 end bidWin," +
//      "case when requestmode=2 and iseffective=1 then 1 else 0 end showNum," +
//      "case when requestmode=3 and iseffective=1 then 1 else 0 end clickNum," +
//      "case when iseffective=1 and isbilling=1 and iswin=1 then winprice else 0 end dsppay," +
//      "case when iseffective=1 and isbilling=1 and iswin=1 then adpayment else 0 end dscost " +
//      "from t_log)ret group by devicetype   ")
//    ret.show()
//    ret.write.mode(SaveMode.Append)
//      .jdbc(getProperties()._2,"Device",getProperties()
//        ._1)


    /**
      * spark core
      */
        import ssc.implicits._
        //数据进行处理
        val rdd: RDD[(String, List[Double])] = df.map(arr => {
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
          val devicetype: Int = arr.getAs[Int]("devicetype")
          val device:String =if(devicetype==1) "手机" else if(devicetype==2) "平板" else "其他"

          (device, RptUtils.request(reqm, proc) ++ RptUtils.click(reqm, iseff) ++ RptUtils.Ad(iseff, isbil, isbid, iswin, ado, winpri, adpay))
        }).rdd

        val ret: RDD[(String, List[Double])] = rdd.reduceByKey((a,b)=>a.zip(b).map(tup=>tup._1+tup._2))
    val res: RDD[(String, Double, Double, Double, Double, Double, Double, Double, Double, Double)] = ret.map(a => (a._1, a._2(0), a._2(1), a._2(2), a._2(5), a._2(6), a._2(3), a._2(4), a._2(7),a._2(8)))


        res.take(20).foreach(println)






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
