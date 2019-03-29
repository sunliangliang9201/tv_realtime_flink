package com.bftv.dt.realtime.model

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.table.expressions.TimeAttribute
import org.apache.flink.util.Collector


/**
  * 分组TopN实现。由于flink自身无论是table api 还是 sql api对于 stream query均没有topn操作，因此需要自己实现。
  *
  * @author sunliangliang 2019-03-10 https://github.com/sunliangliang9201/tv_realtime_flink
  * @version 1.0
  */
//class MyTopNFunction(size: Int) extends ProcessWindowFunction[(String, String, TimeAttribute), (String, Long), String, TimeWindow]{
//
//  override def process(key: String, context: Context, elements: Iterable[(String, String, TimeAttribute)], out: Collector[(String, Long)]): Unit = {
//
//  }
//}
