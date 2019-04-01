package com.bftv.dt.realtime.model

import java.sql.Timestamp

import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * desc
  *
  * @author sunliangliang 2019-04-01 https://github.com/sunliangliang9201/tv_realtime_flink
  * @version 1.0
  */
class WindowResultFunction extends WindowFunction[Long, (Long, String, Long), String, TimeWindow]{

  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[(Long, String, Long)]): Unit = {
    val counts = input.iterator.next()
    out.collect((window.getEnd, key, counts))
  }
}
