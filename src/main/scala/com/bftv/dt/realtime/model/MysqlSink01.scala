package com.bftv.dt.realtime.model

import java.sql.{Connection, PreparedStatement, Timestamp}

import com.bftv.dt.realtime.storage.MysqlManager
import grizzled.slf4j.Logger
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.slf4j.LoggerFactory

/**
  * desc
  *
  * @author sunliangliang 2019-03-10 https://github.com/sunliangliang9201/tv_realtime_flink
  * @version 1.0
  */
class MysqlSink01 extends RichSinkFunction[(Timestamp, String)]{

  val logger = LoggerFactory.getLogger(this.getClass)

  var conn: Connection = null

  var ps: PreparedStatement = null

  override def invoke(value: (Timestamp, String), context: SinkFunction.Context[_]): Unit = {
    try{
      conn = MysqlManager.getMysqlManager.getConnection
      val sql = "insert into bftv.tv_display_window_thirdapp(end_window,topN) values(?,?)"
      ps = conn.prepareStatement(sql)
      ps.setTimestamp(1, value._1)
      ps.setString(2, value._2)
      ps.execute()
    }catch {
      case e: Exception => logger.error("topN sink error ..." + e)
    }finally {
      if (null != ps){
        ps.close()
      }
      if (null != conn){
        conn.close()
      }
    }
  }
}
