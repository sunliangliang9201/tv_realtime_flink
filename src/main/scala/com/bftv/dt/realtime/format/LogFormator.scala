package com.bftv.dt.realtime.format

import com.bftv.dt.realtime.model.Bean

/**
  * triat
  *
  * @author sunliangliang 2019-03-10 https://github.com/sunliangliang9201/tv_realtime_flink
  * @version 1.0
  */
trait LogFormator extends Serializable{

  def format(logStr: String, ipAreaIspCache: Array[(String, String, String, String, Long, Long)], fields: Array[String]): Bean

}
