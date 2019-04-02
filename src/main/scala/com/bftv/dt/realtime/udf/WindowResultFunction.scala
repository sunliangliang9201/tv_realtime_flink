package com.bftv.dt.realtime.udf

import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.types.Row
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
    //先用元祖的方式返回结果，如果后续sink到mysql的时候出问题再回来修改返回类型为Row
//    val row = new Row(3)
//    row.setField(0, window.getEnd)
//    row.setField(1, key)
//    row.setField(2, counts)
    out.collect((window.getEnd, key, counts))
  }
}
