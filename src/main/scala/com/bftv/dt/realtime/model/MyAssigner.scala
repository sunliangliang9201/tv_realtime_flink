package com.bftv.dt.realtime.model

import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark
import org.slf4j.LoggerFactory

import scala.math._

/**
  * 为kafka的message抽取event时间戳以及打水印
  *
  * @author sunliangliang 2019-03-10 https://github.com/sunliangliang9201/tv_realtime_flink
  * @version 1.0
  */
class MyAssigner extends AssignerWithPeriodicWatermarks[Bean]{

  val logger = LoggerFactory.getLogger(this.getClass)

  var currentMaxTimestamp: Long = 0L

  val maxOutOrderness: Long = 10000L

  val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  override def extractTimestamp(element: Bean, previousElementTimestamp: Long): Long = {
    //2019-03-19 10:29:40
    val timeStr = element.itime
    var timestamp = 0L
    //第一种抽取方式
    //为空判断；是否为第一个message就出现问题；itime是否超前很多了；itime是否落后许多了
    //这种抽取时间戳可能不是最好的，但是当前最完善的
//    if (timeStr == "-"){
//      if (currentMaxTimestamp == 0L){
//        return System.currentTimeMillis()
//      }
//      return currentMaxTimestamp
//    }else{
//      try{
//        timestamp = format.parse(timeStr).getTime
//      }catch {
//        case e: Exception => logger.error("parse itime`s timestamp failed ... " + e)
//          timestamp = System.currentTimeMillis()
//      }
//    }
//    if ((timestamp - System.currentTimeMillis()) >= 60000){
//      if (currentMaxTimestamp == 0L){
//        return System.currentTimeMillis()
//      }
//      return currentMaxTimestamp
//    }
//    currentMaxTimestamp = max(currentMaxTimestamp, timestamp)
//    if ((currentMaxTimestamp - timestamp) >= 30000){
//      return currentMaxTimestamp
//    }
//    timestamp

    //第二种抽取方式
    if (timeStr == "-"){
      if (previousElementTimestamp != 0L){
        return previousElementTimestamp
      }else if (currentMaxTimestamp != 0L){
        return currentMaxTimestamp
      }else{
        return System.currentTimeMillis()
      }
    }else{
        try{
          timestamp = format.parse(timeStr).getTime
        }catch {
          case e: Exception => logger.error("parse itime`s timestamp failed ... " + e)
            timestamp = previousElementTimestamp
        }
    }
    if ((timestamp - System.currentTimeMillis()) >= 60000){
      if (previousElementTimestamp != 0L){
        return previousElementTimestamp
      }else if (currentMaxTimestamp != 0L){
        return currentMaxTimestamp
      }else{
        return System.currentTimeMillis()
      }
    }
    currentMaxTimestamp = max(currentMaxTimestamp, timestamp)
    if ((currentMaxTimestamp - timestamp) >= 30000){
      return currentMaxTimestamp
    }
    timestamp
  }

  override def getCurrentWatermark: Watermark = {
    new Watermark(currentMaxTimestamp - maxOutOrderness)
  }
}