package com.bftv.dt.realtime.model

import com.bftv.dt.realtime.format.LogFormator
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import scala.io.Source

/**
  * rich function
  *
  * @author sunliangliang 2019-03-10 https://github.com/sunliangliang9201/tv_realtime_flink
  * @version 1.0
  */
class MyMapFunction(logformator: LogFormator, fields: Array[String]) extends RichMapFunction[String, Bean]{

  var ips: Array[(String, String, String, String, Long, Long)] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val ipsFile = getRuntimeContext.getDistributedCache.getFile("ips")
    ips = Source.fromFile(ipsFile).getLines().toArray.map(line => {
      (line.split("\t")(0), line.split("\t")(1), line.split("\t")(2), line.split("\t")(3), line.split("\t")(4).toLong, line.split("\t")(5).toLong)
    })
  }

  override def map(value: String): Bean = {
    logformator.format(value, ips, fields)
  }
}
