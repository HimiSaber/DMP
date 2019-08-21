package com.utils

object RptUtils {
  //处理请求数
  def request(reqm: Int,proc: Int):List[Double]= {
    var a: Double = 0
    var b: Double = 0
    var c: Double = 0

    if(reqm==1&&proc>=1){
       a = 1
    }else{
      a = 0
    }

    if(reqm==1&&proc>=2){
      b = 1
    }else{
      b = 0
    }

    if(reqm==1&&proc==3){
      c = 1
    }else{
      c = 0
    }

    List(a,b,c)

  }


  //处理展示点击数
  def click(reqm: Int,ise:Int ): List[Double] = {
    var a: Double = 0
    var b: Double = 0
    if(reqm==2&&ise==1){
      a = 1
    }else{
      a = 0
    }
    if(reqm==3&&ise==1){
      b = 1
    }else{
      b = 0
    }

    List(a,b)
  }


  //处理竞价操作
  def Ad(ise: Int,
         isb: Int,
         isBid: Int,
         isWin: Int,
         ado: Int,
         wp: Double,
         adp:Double):List[Double]={
    var a: Double = 0
    var b: Double = 0
    var c: Double = 0
    var d: Double = 0

    if(ise==1 && isb==1 && isBid==1){
      a = 1
    }else{
      a = 0
    }

    if(ise==1 && isb==1 && isWin==1 && ado !=1){
      b = 1
    }else{
      b = 0
    }
    if(ise==1 && isb==1 && isWin==1 ){
      c = 1
    }else{
      c = 0
    }
    if(ise==1 && isb==1 && isWin==1 ){
      d = 1
    }else{
      d = 0
    }
    List(a,b,c/1000,d/1000)
  }

}
