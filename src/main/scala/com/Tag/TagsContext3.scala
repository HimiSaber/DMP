package com.Tag

import java.util.concurrent.atomic.LongAccumulator

import com.typesafe.config.{Config, ConfigFactory}
import com.utils.{HbaseUtils, JedisUtils, TagUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import redis.clients.jedis.Jedis
import org.apache.hadoop.hbase.NamespaceDescriptor
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
object TagsContext3 {

  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      println("目录不匹配，退出程序")
      sys.exit()
    }


    val Array(inputPath, stopPath, outputPath, date) = args

    //创建上下文
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
    hdConf.set("hbase.master", config.getString("hbase.master"))
    hdConf.set("hbase.zookeeper.quorum", config.getString("hbase.host"))
    //创建HbaseConnection
    val hbconn: Connection = ConnectionFactory.createConnection(hdConf)

    //    val conf: Configuration = HBaseConfiguration.create()
    //    conf.set("hbase.master",config.getString("hbase.master"))
    //    conf.set("hbase.zookeeper.quorum",config.getString("hbase.host"))
    //    val hbconn: Connection = ConnectionFactory.createConnection(conf)

    // val hbconn: Connection = HbaseUtils.getConn()


    val admin: Admin = hbconn.getAdmin
    if (!admin.tableExists(TableName.valueOf(tbname))) {
      //创建表操作
      val tableDe = new HTableDescriptor(TableName.valueOf(tbname))
      val colDesc = new HColumnDescriptor("tag")
      tableDe.addFamily(colDesc)
      admin.createTable(tableDe)
      admin.close()
      hbconn.close()
    }
    val jobconf = new JobConf(hdConf)

    jobconf.setOutputFormat(classOf[TableOutputFormat])
    jobconf.set(TableOutputFormat.OUTPUT_TABLE, tbname)


    //停用词库
    val stopword = ssc.sparkContext.textFile(stopPath).map((_, 0)).collectAsMap()
    val bcstopword = ssc.sparkContext.broadcast(stopword)
    //打标签
    val df: DataFrame = ssc.read.parquet(inputPath)
    val baseRDD: RDD[(List[String], Row)] = df.filter(TagUtils.OneUserId)
      .rdd.mapPartitions(part => {
      part.map(row => {
        //接下来所有的标签都在内部实现
        //取出用户id(key)
        val userId: List[String] = TagUtils.getAllUserId(row)

        (userId, row)
      })
    })

    //点的集合
    val point: RDD[(Long, List[(String, Int)])] = baseRDD.mapPartitions(part => {
      val jedis: Jedis = JedisUtils.getJedis()
      try {
        part.flatMap(r => {
          //接下来所有的标签都在内部实现
          //通过Row数据打标签
          val row: Row = r._2
          val idList: List[String] = r._1
          val adList: List[(String, Int)] = TagsAd.makeTags(row)
          val appList: List[(String, Int)] = TagsApp.makeTags(row, jedis)
          val canList: List[(String, Int)] = TagsCannal.makeTags(row)
          val devList: List[(String, Int)] = TagsDevice.makeTags(row)
          val keyList: List[(String, Int)] = TagsKey.makeTags(row, bcstopword)
          val areaList: List[(String, Int)] = TagsArea.makeTags(row)
          val busList: List[(String, Int)] = TagBusiness.makeTags(row, jedis)
          val alltags: List[(String, Int)] = adList ++ appList ++ canList ++ devList ++ keyList ++ areaList ++ busList
          idList.map(uid => {
            if (idList.head.equals(uid)) {
              (uid.hashCode.toLong, alltags)
            } else {
              (uid.hashCode.toLong, List.empty)
            }
          })
        })
        //tags.saveAsTextFile(outputPath))
      }
    })

    //point.take(20).foreach(println)
    //边的集合


    val edge: RDD[Edge[Int]] = baseRDD.flatMap(tp => {
      tp._1.map(uid => Edge(tp._1.hashCode().toLong, uid.hashCode.toLong, 0))
    })
    //edge.take(20).foreach(println)
    //构件图
    val graph = Graph(point,edge)
    //取出顶点，图里的连通图
    val vertices: VertexRDD[VertexId] = graph.connectedComponents().vertices


    //处理所有的标签和id
    vertices.join(point).map({
      case (uid,(conId,tagsAll))=>(conId,tagsAll)
    }).reduceByKey((list1,list2)=>{
      //聚合所有的标签
      (list1++list2).groupBy(_._1).mapValues(_.map(_._2).sum).toList
    }).take(20).foreach(println)


    ssc.stop()




    //聚合
    //    val ret: RDD[(ImmutableBytesWritable, Put)] = tags.reduceByKey((li1, li2) => {
    //      //List(("LN插屏",1),("LN全屏"，1),("ZC沈阳",1),())
    //      (li1 ::: li2)
    //        //List(("LN插屏",List(1,1,1,1)),())
    //        .groupBy(_._1)
    //        .mapValues(_.foldLeft[Int](0)(_ + _._2))
    //        .toList
    //    }).map {
    //      case (userid, userTag) => {
    //        val put = new Put(Bytes.toBytes(userid))
    //        //处理以下标签
    //        val tags: String = userTag.map(tup => tup._1 + "," + tup._2).mkString(",")
    //        put.addImmutable(Bytes.toBytes("tag"), Bytes.toBytes(date), Bytes.toBytes(tags))
    //        (new ImmutableBytesWritable(), put)
    //      }
    //
    //    }


    //ret.saveAsHadoopDataset(jobconf)
    //    def longAccumulator(name: String): LongAccumulator = {
    //      val acc = LongAccumulator
    //
    //      ssc.sparkContext.register(acc,"a")
    //      acc
    //    }

  }
}