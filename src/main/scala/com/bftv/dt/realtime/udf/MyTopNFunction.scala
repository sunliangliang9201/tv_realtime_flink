package com.bftv.dt.realtime.udf

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.types.Row
import org.apache.flink.util.Collector


/**
  * 分组TopN实现。由于flink自身无论是table api 还是 sql api对于 stream query均没有topn操作，因此需要自己实现。
  *
  * @author sunliangliang 2019-03-10 https://github.com/sunliangliang9201/tv_realtime_flink
  * @version 1.0
  */
class MyTopNFunction(size: Int = 5) extends ProcessFunction[Row, Row]{

  override def open(parameters: Configuration): Unit = {
    println(1)
    super.open(parameters)
  }

  override def processElement(value: Row, ctx: ProcessFunction[Row, Row]#Context, out: Collector[Row]): Unit = {
    println(2)
  }

  override def onTimer(timestamp: Long, ctx: ProcessFunction[Row, Row]#OnTimerContext, out: Collector[Row]): Unit = {
    print(3)
    super.onTimer(timestamp, ctx, out)
  }
}