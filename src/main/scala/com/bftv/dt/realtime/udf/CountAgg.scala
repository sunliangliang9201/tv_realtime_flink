package com.bftv.dt.realtime.udf

import com.bftv.dt.realtime.model.Bean
import org.apache.flink.api.common.functions.AggregateFunction

/**
  * desc
  *
  * @author sunliangliang 2019-03-10 https://github.com/sunliangliang9201/tv_realtime_flink
  * @version 1.0
  */
class CountAgg extends AggregateFunction[Bean, Long, Long]{

  override def add(value: Bean, accumulator: Long): Long = accumulator + 1

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}
