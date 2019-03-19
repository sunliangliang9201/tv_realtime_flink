package com.bftv.dt.realtime.format

import java.net.URLDecoder

import com.alibaba.fastjson.JSON
import com.bftv.dt.realtime.model.Bean
import com.bftv.dt.realtime.utils.IPParser
import org.slf4j.LoggerFactory
import scala.collection.mutable

/**
  * 解析日志message
  *
  * @author sunliangliang 2019-03-10 https://github.com/sunliangliang9201/tv_realtime_flink
  * @version 1.0
  */
class SDKFormator extends LogFormator {

  val logger = LoggerFactory.getLogger(this.getClass)

  val logRegex = """(\d+\.\d+\.\d+\.\d+).*?logger.php\?(.*?) HTTP.*""".r

  /**
    * 解析整个message并返回一个json字符串
    * @param logStr 原始message
    * @param ipAreaIspCache 解析用的ipareaisp.txt
    * @param fields 需要解析出来的字段
    * @return json字符串
    */
  override def format(logStr: String, ipAreaIspCache: Array[(String, String, String, String, Long, Long)], fields: Array[String]): Bean = {
    var paramMap: mutable.Map[String, String] = mutable.Map[String, String]()
    var res :Map[String, String] = Map[String, String]()
    var bean: Bean = null
    var appkey = "-"
    try{
      val logRegex(ip, query) = logStr
      val iparea = IPParser.parse(ip,ipAreaIspCache)
      paramMap += "country" -> iparea._1
      paramMap += "province" -> iparea._2
      paramMap += "city" -> iparea._3
      paramMap += "isp" -> iparea._4
      val fieldsLogList = query.split("&").toList
      fieldsLogList.map(x => paramMap += x.split("=")(0) -> x.split("=")(1))
      if(!paramMap.getOrElse("enc", "0").equals("0")){
        paramMap("log") = decode(paramMap("log"))
        paramMap("ltype") = decode(paramMap("ltype"))
      }else{
        paramMap("log") = URLDecoder.decode(paramMap("log"), "utf-8")
      }
      val allJson = JSON.parseObject(paramMap("log"))
      val time = allJson.get("itime").toString
      appkey = paramMap.getOrElse("appkey", "-")
      if ("-" != appkey){
        for (i <- fields){
          i match {
            case "country" => res += i -> paramMap.getOrElse("country", "-")
            case "province" => res += i -> paramMap.getOrElse("province", "-")
            case "city" => res += i -> paramMap.getOrElse("city", "-")
            case "isp" => res += i -> paramMap.getOrElse("isp", "-")
            case "appkey" => res += i -> paramMap.getOrElse("appkey", "-")
            case "ltype" => res += i -> paramMap.getOrElse("ltype", "-")
            case "value" => res += i -> allJson.get("value").toString
            case "dt" => res += i -> time.split(" ")(0)
            case "hour" =>res += i -> time.split(" ")(1).split(":")(0)
            case "mins" =>res += i -> time.split(" ")(1).split(":")(1)
            case _ => res += i -> get2Json(allJson, i)
          }
        }
      }
      bean = new Bean(
        country = res.getOrElse("country", "-"),
        province = res.getOrElse("province", "-"),
        city = res.getOrElse("city", "-"),
        isp = res.getOrElse("isp", "-"),
        appkey = res.getOrElse("appkey", "-"),
        ltype = res.getOrElse("ltype", "-"),
        uid = res.getOrElse("uid", "-"),
        imei = res.getOrElse("imei", "-"),
        userid = res.getOrElse("userid", "-"),
        mac = res.getOrElse("mac", "-"),
        apptoken = res.getOrElse("apptoken", "-"),
        ver = res.getOrElse("ver", "-"),
        mtype = res.getOrElse("mtype", "-"),
        version = res.getOrElse("version", "-"),
        androidid = res.getOrElse("androidid", "-"),
        unet = res.getOrElse("unet", "-"),
        mos = res.getOrElse("mos", "-"),
        itime = res.getOrElse("itime", "-"),
        uuid = res.getOrElse("uuid", "-"),
        gid = res.getOrElse("gid", "-"),
        sn = res.getOrElse("sn", "-"),
        plt_ver = res.getOrElse("plt_ver", "-"),
        package_name = res.getOrElse("package_name", "-"),
        pid = res.getOrElse("pid", "-"),
        lau_ver = res.getOrElse("lau_ver", "-"),
        plt = res.getOrElse("plt", "-"),
        softid = res.getOrElse("softid", "-"),
        page_title = res.getOrElse("page_title", "-"),
        ip = res.getOrElse("ip", "-"),
        value = res.getOrElse("value", "-")
      )
      return bean
    }catch {
      case e: Exception => logger.error("Parse the message failed ..., " + e)
    }
    bean
  }

  /**
    * 存在二级json的问题，一般value中不取出来，如果取出来就会出现空指针，此时去除二级json即可
    * @param allJson log的Json
    * @param field 字段key
    * @return 返回一级或者二级的value
    */
  def get2Json(allJson: com.alibaba.fastjson.JSONObject, field: String): String ={
    var res = "-"
    try{
      res = allJson.get(field).toString
    }catch {
      case e: Exception => {
        try{
          res = JSON.parseObject(allJson.get("value").toString).get(field).toString
        }catch {
          case e: Exception => logger.error("Parse the massage from the json(log) & json(value) failed for field name of " + field + "," + e)
        }
      }
    }
    return res
  }

  /**
    * 如果㤇解析的话，解密log或者ltype
    * @param logStr 原始日志
    * @return 解密后日志
    */
  def decode(logStr: String): String = {
    val decryptStr = " !_#$%&'()*+,-.ABCDEFGHIJKLMNOP?@/0123456789:;<=>QRSTUVWXYZ[\\]^\"`nopqrstuvwxyzabcdefghijklm{|}~"
    var resLogStr = ""
    val realLog = URLDecoder.decode(logStr, "utf-8")
    for(i <- 0 to realLog.length - 1){
      var ch = realLog.charAt(i)
      if(ch.toInt >= 32 && ch.toInt <= 126){
        resLogStr +=  decryptStr.charAt(ch.toInt - 32)
      }else{
        resLogStr += ch
      }
    }
    resLogStr
  }
}
