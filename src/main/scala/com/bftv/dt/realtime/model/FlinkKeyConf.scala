package com.bftv.dt.realtime.model

/**
  * conf object
  *
  * @author sunliangliang 2019-03-10 https://github.com/sunliangliang9201/tv_realtime_flink
  * @version 1.0
  */

case class FlinkKeyConf (
                          flinkKey:String,
                          appName:String,
                          driverCores:String,
                          formator:String,
                          topics:String,
                          groupID:String,
                          tableName:String,
                          fields:Array[String],
                          brolerList:String
                        )
