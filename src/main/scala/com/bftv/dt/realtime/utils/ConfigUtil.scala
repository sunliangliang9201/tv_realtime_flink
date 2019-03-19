package com.bftv.dt.realtime.utils

import com.typesafe.config.{Config, ConfigFactory}

/**
  * 返回resources下application.properties中的config对象
  *
  * @author sunliangliang 2019-03-10 https://github.com/sunliangliang9201/tv_realtime_flink
  * @version 1.0
  */
object ConfigUtil {

  val conf = ConfigFactory.load("application.properties")

  def getConf: Option[Config] ={
    Some(conf)
  }

}
