package com.utils

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object TagUtils {

  //过滤需要的字段
  val OneUserId =
    """
      |imei != '' or mac != '' or openudid != '' or androidid != '' or idfa != '' or
      |imeimd5 != '' or macmd5 != '' or openudidmd5 != '' or androididmd5 != '' or idfamd5 != '' or
      |imeisha1 != '' or macsha1 != '' or openudidsha1 != '' or androididsha1 != '' or idfasha1 != ''
    """.stripMargin

  def getOneUserId(row: Row):String ={
    row match {
      case v if StringUtils.isNotBlank(v.getAs[String]("imei")) => v.getAs[String]("imei")
      case v if StringUtils.isNotBlank(v.getAs[String]("mac")) => v.getAs[String]("mac")
      case v if StringUtils.isNotBlank(v.getAs[String]("openudid")) => v.getAs[String]("openudid")
      case v if StringUtils.isNotBlank(v.getAs[String]("androidid")) => v.getAs[String]("androidid")
      case v if StringUtils.isNotBlank(v.getAs[String]("idfa")) => v.getAs[String]("idfa")

      case v if StringUtils.isNotBlank(v.getAs[String]("imeimd5")) => v.getAs[String]("imeimd5")
      case v if StringUtils.isNotBlank(v.getAs[String]("macmd5")) => v.getAs[String]("macmd5")
      case v if StringUtils.isNotBlank(v.getAs[String]("openudidmd5")) => v.getAs[String]("openudidmd5")
      case v if StringUtils.isNotBlank(v.getAs[String]("androididmd5")) => v.getAs[String]("androididmd5")
      case v if StringUtils.isNotBlank(v.getAs[String]("idfamd5")) => v.getAs[String]("idfamd5")

      case v if StringUtils.isNotBlank(v.getAs[String]("imeisha1")) => v.getAs[String]("imeisha1")
      case v if StringUtils.isNotBlank(v.getAs[String]("macsha1")) =>v.getAs[String]("macsha1")
      case v if StringUtils.isNotBlank(v.getAs[String]("openudidsha1")) =>v.getAs[String]("openudidsha1")
      case v if StringUtils.isNotBlank(v.getAs[String]("androididsha1")) => v.getAs[String]("androididsha1")
      case v if StringUtils.isNotBlank(v.getAs[String]("idfasha1")) => v.getAs[String]("idfasha1")



    }
  }

}
