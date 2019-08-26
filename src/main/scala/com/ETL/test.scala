package com.ETL

import com.utils.GmapUtils
import org.apache.spark.sql.SparkSession

object test {

  def main(args: Array[String]): Unit = {

    val ssc: SparkSession = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()




  }

}
