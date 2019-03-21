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
                 var value: String = "-"
//                 sn:String = "-",
//                 plt_ver: String = "-",
//                 package_name: String = "-",
//                 pid: String = "-",
//                 lau_ver: String = "-",
//                 plt: String = "-",
//                 softid: String = "-",
//                 page_title: String = "-",
//                 ip: String = "-",

                )
object App{
  def main(args: Array[String]): Unit = {
    var b = Bean()
    b.country = "a"
  }
}