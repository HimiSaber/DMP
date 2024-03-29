package com.Tag

import com.typesafe.config.{Config, ConfigFactory}
import com.utils.{JedisUtils, TagUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

object TagsContext {
  def main(args: Array[String]): Unit = {
    if(args.length != 2) {
      println("目录不匹配，退出程序")
      sys.exit()
    }


//    val Array(inputPath,outputPath,appinfoPath,unPath,date)=args

    val Array(inputPath,date)=args

    //创建上下文
//    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
//    new SparkContext(conf)
    val ssc: SparkSession = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    // todo 调用HbaseAPI
    val config: Config = ConfigFactory.load()
    //表名字
    val tbname: String = config.getString("hbase.TableName")

    //创建hadoop任务
    val hdConf: Configuration = ssc.sparkContext.hadoopConfiguration
    hdConf.set("hbase.master",config.getString("hbase.master"))
    hdConf.set("hbase.zookeeper.quorum",config.getString("hbase.host"))
    hdConf.set("hbase.zookeeper.property.clientPort",config.getString("hbase.port"))
    //创建HbaseConnection
    val hbconn: Connection = ConnectionFactory.createConnection(hdConf)

    val admin: Admin = hbconn.getAdmin
    if(!admin.tableExists(TableName.valueOf(tbname))){
      //创建表操作
      val tableDe = new HTableDescriptor(TableName.valueOf(tbname))
      val colDesc = new HColumnDescriptor("tag")
      tableDe.addFamily(colDesc)
      admin.createTable(tableDe)
      admin.close()
      hbconn.close()
    }
    val jobconf = new JobConf()

    jobconf.setOutputFormat(classOf[TableOutputFormat])
    jobconf.set(TableOutputFormat.OUTPUT_TABLE,tbname)



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
          val adList: List[(String, Int)] = TagsAd.makeTags(row,jedis)

          (userId, adList)
        })
      }finally {
        jedis.close()
      }
    })

tags.reduceByKey((li1, li2) => {
      //List(("LN插屏",1),("LN全屏"，1),("ZC沈阳",1),())
      (li1 ::: li2)
        //List(("LN插屏",List(1,1,1,1)),())
        .groupBy(_._1)
        .mapValues(_.foldLeft[Int](0)(_ + _._2))
        .toList
    }).map{
      case(userid,userTag)=>{
        val put = new Put(Bytes.toBytes(userid))
        //处理以下标签
        val tags: String = userTag.map(tup=>tup._1+","+tup._2).mkString(",")
        put.addImmutable(Bytes.toBytes("tags"),Bytes.toBytes(date),Bytes.toBytes(tags))
        (new ImmutableBytesWritable(),put)
      }

    }.saveAsHadoopDataset(jobconf)


  }

}
