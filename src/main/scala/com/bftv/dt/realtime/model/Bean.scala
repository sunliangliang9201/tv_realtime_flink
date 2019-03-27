package com.bftv.dt.realtime.model

/**
  * desc
  *country,province,city,isp,appkey,ltype,uid,imei,userid,mac,apptoken,ver,mtype,version,androidid,unet,mos,itime,uuid,gid,sn,plt_ver,package_name,pid,lau_ver,plt,softid,page_title,ip,value
 *
  * @author sunliangliang 2019-03-10 https://github.com/sunliangliang9201/tv_realtime_flink
  * @version 1.0
  */

case class Bean (var country: String = "-",
                 var province: String = "-",
                 var city: String = "-",
                 var isp: String = "-",
                 var appkey: String = "-",
                 var ltype: String = "-",
                 var uid: String = "-",
                 var imei: String = "-",
                 var userid: String = "-",
                 var mac: String = "-",
                 var apptoken: String = "-",
                 var ver: String = "-",
                 var mtype:String = "-",
                 var version: String = "-",
                 var androidid: String = "-",
                 var unet:String = "-",
                 var mos: String = "-",
                 var itime: String = "-",
                 var uuid: String = "-",
                 var gid:String = "-",
                 var jsonvalue: String = "-",
                 var sn:String = "-",
                 var plt_ver: String = "-",
                 var package_name: String = "-",
                 var pid: String = "-",
                 var lau_ver: String = "-",
                 var plt: String = "-",
                 var softid: String = "-",
                 var page_title: String = "-",
                 var ip: String = "-"
                )
object App{
  def main(args: Array[String]): Unit = {
    var b = Bean()
    b.country = "a"
  }
}