package com.bftv.dt.realtime.model

import java.text.SimpleDateFormat
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark
import scala.math._

/**
  * 为kafka的message抽取event时间戳以及打水印
  *
  * @author sunliangliang 2019-03-10 https://github.com/sunliangliang9201/tv_realtime_flink
  * @version 1.0
  */
class MyAssigner extends AssignerWithPeriodicWatermarks[Bean]{

  var currentMaxTimestamp: Long = 0L

  val maxOutOrderness: Long = 5000L

  val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  override def extractTimestamp(element: Bean, previousElementTimestamp: Long): Long = {
    //2019-03-19 10:29:40
    val timeStr = element.itime
    var timestamp = 0L
    if (timeStr == "-"){
      timestamp = previousElementTimestamp
    }else{
      timestamp = format.parse(timeStr).getTime
    }
    currentMaxTimestamp = max(currentMaxTimestamp, timestamp)
    if ((currentMaxTimestamp - timestamp) > 30000){
      return currentMaxTimestamp
    }
    timestamp
    //System.currentTimeMillis()
  }

  override def getCurrentWatermark: Watermark = {
    new Watermark(currentMaxTimestamp - maxOutOrderness)
    //new Watermark(System.currentTimeMillis())
  }
}
