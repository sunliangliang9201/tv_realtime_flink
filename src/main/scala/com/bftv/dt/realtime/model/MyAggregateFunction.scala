package com.bftv.dt.realtime.model


import org.apache.flink.table.functions.AggregateFunction
import scala.collection.mutable.Set

/**
  * 自定义聚合函数，目的是当日期切换的时候清楚昨天的数据
  *
  * @author sunliangliang 2019-03-10 https://github.com/sunliangliang9201/tv_realtime_flink
  * @version 1.0
  */
class MyAggregateFunction extends AggregateFunction[Long, CountAccum]{

  override def createAccumulator(): CountAccum = {
    CountAccum(Set[String]())
  }

  override def getValue(accumulator: CountAccum): Long = {
    accumulator.countsSet.size
  }

  def accumulate(accumulator: CountAccum, dt: String, uuidStr: String): Unit ={
    if (accumulator.currentDt == "" || dt > accumulator.currentDt){
      accumulator.countsSet.clear()
      accumulator.currentDt = dt
      accumulator.countsSet += uuidStr
    }else if (dt == accumulator.currentDt){
      accumulator.countsSet += uuidStr
    }
  }

  def merge(accumulator: CountAccum, iter: Iterable[CountAccum]): Unit ={
    val it = iter.iterator
    while (it.hasNext){
      val accum = it.next()
      accumulator.countsSet.union(accum.countsSet)
    }
  }

  def reSet(accumulator: CountAccum) :CountAccum ={
    accumulator.countsSet.clear()
    accumulator
  }
}

case class CountAccum(var countsSet: Set[String], var currentDt: String = "")