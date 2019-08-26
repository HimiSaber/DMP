package com.utils

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.hadoop.hbase.executor.ExecutorService


object HbaseUtils {


   private val conf: Configuration = HBaseConfiguration.create()
  private val configValues = ConfigFactory.load()
//  conf.set("hbase.zookeeper.quorum", configValues.getString("hbase.zookeeper.quorum"))
//  conf.set("hbase.zookeeper.property.clientPort", configValues.getString("hbase.zookeeper.property.clientPort"))
//  conf.set("hbase.defaults.for.version.skip", configValues.getString("hbase.zookeeper.property.clientPort"))
//  conf.set("hbase.master",configValues.getString("hbase.master"))
//  conf.set("hbase.zookeeper.quorum",configValues.getString("hbase.host"))
  private val conn: Connection = ConnectionFactory.createConnection(conf)
  def getConn():Connection={
    conn
  }
}
