package com.bftv.dt.realtime.model

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.table.functions.AggregateFunction

import scala.collection.mutable.Set
/**
  * 自定义聚合函数，目的是当日期切换的时候清楚昨天的数据
  *
  * @author sunliangliang 2019-03-10 https://github.com/sunliangliang9201/tv_realtime_flink
  * @version 1.0
  */
class MyAggregateFunction extends AggregateFunction[Long, CountAccum]{

  val format = new SimpleDateFormat("yyyy-MM-dd")
  var currentDtTimestamp = 0L

  override def createAccumulator(): CountAccum = {
    CountAccum()
  }

  override def getValue(accumulator: CountAccum): Long = {
    accumulator.uuidSet.size
  }

  def accumulate(accumulator: CountAccum, dt: String, uuid: String): Unit ={
    val timestamp = format.parse(dt).getTime
    if (timestamp > currentDtTimestamp){
      accumulator.uuidSet.clear()
      currentDtTimestamp = timestamp
      accumulator.uuidSet += uuid
    }else if (timestamp == currentDtTimestamp){
      accumulator.uuidSet += uuid
    }
  }

  def merge(accumulator: CountAccum, iter: Iterable[CountAccum]): Unit ={
    val it = iter.iterator
    while (it.hasNext){
      val accum = it.next()
      accumulator.uuidSet.union(accum.uuidSet)
    }
  }

  def reSet(accumulator: CountAccum) :CountAccum ={
    accumulator.uuidSet.clear()
    accumulator
  }
}

case class CountAccum(val uuidSet: Set[String] = Set())
