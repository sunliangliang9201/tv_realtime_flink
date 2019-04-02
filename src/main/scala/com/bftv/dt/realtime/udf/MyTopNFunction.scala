package com.bftv.dt.realtime.udf

import java.sql.Timestamp

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source


/**
  * 分组TopN实现。由于flink自身无论是table api 还是 sql api对于 stream query均没有topn操作，因此需要自己实现。
  *
  * @author sunliangliang 2019-03-10 https://github.com/sunliangliang9201/tv_realtime_flink
  * @version 1.0
  */
class MyTopNFunction(size: Int = 5) extends KeyedProcessFunction[Long, (Long, String, Long), (Timestamp, String)]{

  var arr: ArrayBuffer[(String, Long)] = _

  var res: mutable.StringBuilder = _

  override def open(parameters: Configuration): Unit = {
    arr = ArrayBuffer[(String, Long)]()
    res = new mutable.StringBuilder()
    super.open(parameters)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, (Long, String, Long), (Timestamp, String)]#OnTimerContext, out: Collector[(Timestamp, String)]): Unit = {
    val elements = arr.sortBy(_._2).reverse.take(size)
    res.clear()
    res.append("{")
    for (i <- elements){
      res.append("\"").append(i._1).append("\"").append(":").append(i._2.toString).append(",")
    }
    res.substring(0, res.length - 1)
    res.append("}")
    elements.clear()
    out.collect((new Timestamp(timestamp - 1), res.toString()))
  }

  override def processElement(value: (Long, String, Long), ctx: KeyedProcessFunction[Long, (Long, String, Long), (Timestamp, String)]#Context, out: Collector[(Timestamp, String)]): Unit = {
    arr.append((value._2, value._3))
    ctx.timerService().registerEventTimeTimer(value._1 + 1)
  }
}