package com.bftv.dt.realtime.udf

import com.bftv.dt.realtime.format.LogFormator
import com.bftv.dt.realtime.model.Bean
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration

import scala.collection.mutable
import scala.io.Source

/**
  * rich function
  *
  * @author sunliangliang 2019-03-10 https://github.com/sunliangliang9201/tv_realtime_flink
  * @version 1.0
  */
class MyMapFunction(logformator: LogFormator, fields: Array[String]) extends RichMapFunction[String, Bean]{

  var ips: Array[(String, String, String, String, Long, Long)] = _

  //var map: mutable.Map[String, String] = mutable.Map[String ,String]()

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val ipsFile = getRuntimeContext.getDistributedCache.getFile("ips")
//    val page_titlesFile = getRuntimeContext.getDistributedCache.getFile("page_titles")
//    for (i <- Source.fromFile(page_titlesFile).getLines()){
//      map += (i.split(",")(0) -> i.split(",")(1))
//    }
    ips = Source.fromFile(ipsFile, "utf8").getLines().toArray.map(line => {
      (line.split("\t")(0), line.split("\t")(1), line.split("\t")(2), line.split("\t")(3), line.split("\t")(4).toLong, line.split("\t")(5).toLong)
    })
  }

  override def map(value: String): Bean = {
//    val bean = logformator.format(value, ips, fields)
//    bean.page_title = map.getOrElse(bean.page_title, "-")
//    bean
    logformator.format(value, ips, fields)
  }
}
