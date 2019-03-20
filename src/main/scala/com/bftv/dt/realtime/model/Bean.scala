package com.bftv.dt.realtime.model

/**
  * desc
  *country,province,city,isp,appkey,ltype,uid,imei,userid,mac,apptoken,ver,mtype,version,androidid,unet,mos,itime,uuid,gid,sn,plt_ver,package_name,pid,lau_ver,plt,softid,page_title,ip,value
 *
  * @author sunliangliang 2019-03-10 https://github.com/sunliangliang9201/tv_realtime_flink
  * @version 1.0
  */

case class Bean (country: String = "-",
                 province: String = "-",
                 city: String = "-",
                 isp: String = "-",
                 appkey: String = "-",
                 ltype: String = "-",
                 uid: String = "-",
                 imei: String = "-",
                 userid: String = "-",
                 mac: String = "-",
                 apptoken: String = "-",
                 ver: String = "-",
                 mtype:String = "-",
                 version: String = "-",
                 androidid: String = "-",
                 unet:String = "-",
                 mos: String = "-",
                 itime: String = "-",
                 uuid: String = "-",
                 gid:String = "-",
//                 sn:String = "-",
//                 plt_ver: String = "-",
//                 package_name: String = "-",
//                 pid: String = "-",
//                 lau_ver: String = "-",
//                 plt: String = "-",
//                 softid: String = "-",
//                 page_title: String = "-",
//                 ip: String = "-",
                 value: String = "-"
                )
object App{
  def main(args: Array[String]): Unit = {
    val b = Bean(appkey = "q")
    println(b)
  }
}