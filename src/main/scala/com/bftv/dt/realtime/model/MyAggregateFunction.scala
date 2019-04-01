package com.bftv.dt.realtime.model


import com.bftv.dt.realtime.utils.MyBloomFilter
import org.apache.flink.table.functions.AggregateFunction


/**
  * 自定义聚合函数，目的是当日期切换的时候清楚昨天的数据
  *
  * @author sunliangliang 2019-03-10 https://github.com/sunliangliang9201/tv_realtime_flink
  * @version 1.0
  */
class MyAggregateFunction extends AggregateFunction[Long, CountAccum]{

  //这里做个备份
  //select HOP_END(rowtime, INTERVAL '5' minute, INTERVAL '1' day) as end_window, myAggreOne(cast(DATE_FORMAT(rowtime, '%Y-%m-%d') as varchar), uuid) as counts from tv_heart group by HOP(rowtime, INTERVAL '5' minute, INTERVAL '1' day)

  override def createAccumulator(): CountAccum = {
    CountAccum(new MyBloomFilter)
  }

  override def getValue(accumulator: CountAccum): Long = {
    accumulator.counts
  }

  def accumulate(accumulator: CountAccum, dt: String, uuidStr: String): Unit ={
    if (accumulator.currentDt == "" || dt > accumulator.currentDt){
      accumulator.counts = 0L
      accumulator.currentDt = dt
      accumulator.bloomFilter.bitSet.clear()
      accumulator.bloomFilter.hashValue(uuidStr)
      accumulator.counts += 1
    }else if (dt == accumulator.currentDt){
      if (!accumulator.bloomFilter.exists(uuidStr)){
        accumulator.bloomFilter.hashValue(uuidStr)
        accumulator.counts += 1
      }
    }
  }

  def merge(accumulator: CountAccum, iter: Iterable[CountAccum]): Unit ={
    val it = iter.iterator
    while (it.hasNext){
      val accum = it.next()
      accumulator.counts += accum.counts
      accumulator.bloomFilter.bitSet.andNot(accum.bloomFilter.bitSet)
    }
  }

  def reSet(accumulator: CountAccum) :CountAccum ={
    accumulator.bloomFilter.bitSet.clear()
    accumulator.counts = 0L
    accumulator
  }
}

case class CountAccum(bloomFilter: MyBloomFilter, var currentDt: String = "", var counts: Long = 0L)