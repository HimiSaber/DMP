package com.utils

import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.util.EntityUtils

/**
  * Http请求协议，
  */
object HttpUtils {

  //get
  def get(url:String):String = {
    val client: CloseableHttpClient = HttpClients.createDefault()

    val get = new HttpGet(url)


    //发送请求
    val response: CloseableHttpResponse = client.execute(get)

    EntityUtils.toString(response.getEntity,"UTF-8")
  }
}
