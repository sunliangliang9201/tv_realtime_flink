package com.bftv.dt.realtime.model

import java.sql.{Date, Timestamp}
import com.bftv.dt.realtime.storage.MysqlDao
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink
import org.apache.flink.streaming.api.scala._
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

/**
  * 返回JDBCSink
  *
  * @author sunliangliang 2019-03-10 https://github.com/sunliangliang9201/tv_realtime_flink
  * @version 1.0
  */
class JDBCSinkFactory {

  var maps = Map[String, TypeInformation[_]]()
  maps += ("STRING_TYPE" -> createTypeInformation[String])
  maps += ("INT_TYPE" -> createTypeInformation[Int])
  maps += ("VARCHAR_TYPE" -> createTypeInformation[String])
  maps += ("VARCHAR2_TYPE" -> createTypeInformation[String])
  maps += ("TIMESTAMP_TYPE" -> createTypeInformation[Timestamp])
  maps += ("LONG_TYPE" -> createTypeInformation[Long])
  maps += ("CHAR_TYPE" -> createTypeInformation[String])
  maps += ("FLOAT_TYPE" -> createTypeInformation[Float])
  maps += ("BIGINT_TYPE" -> createTypeInformation[Long])
  maps += ("DATE_TYPE" -> createTypeInformation[Date])

  val logger = LoggerFactory.getLogger(this.getClass)

  /**
    * 获取sink
    * @param flinkQuery info object
    * @return
    */
  def getJDBCSink(flinkQuery: FlinkQuery): (JDBCAppendTableSink, Array[(String, TypeInformation[_])]) ={
    val fieldsType: Array[(String, String)] = MysqlDao.getTableInfo(flinkQuery)
    val arrTypes = ArrayBuffer[(String, TypeInformation[_])]()
    for (i <- 0 until fieldsType.length -1 ){
      val typeName = fieldsType(i)._2.split("\\(")(0).toUpperCase + "_TYPE"
      arrTypes.append((fieldsType(i)._1, maps.getOrElse(typeName, createTypeInformation[String])))
    }
    var tmpSink: JDBCAppendTableSink = null
    try {
      tmpSink = JDBCAppendTableSink.builder()
        .setDrivername("com.mysql.jdbc.Driver")
        .setDBUrl("jdbc:mysql://" + flinkQuery.db_host + ":" + flinkQuery.db_port + "/" + flinkQuery.db_name)
        .setQuery(flinkQuery.insert_sql)
        .setUsername(flinkQuery.db_user)
        .setPassword(flinkQuery.db_pwd)
        .setParameterTypes(arrTypes.map(_._2).toArray: _*)
        .build()
      return (tmpSink, arrTypes.toArray)
    }catch {
      case e: Exception => logger.error("initial JDBCSink failed ... + " + e)
    }
    (tmpSink, arrTypes.toArray)
  }

  def main(args: Array[String]): Unit = {
    getJDBCSink(MysqlDao.getQueryConfig("TvDisplay")(0))
  }
}
