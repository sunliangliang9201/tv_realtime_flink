package com.bftv.dt.realtime.storage

import java.sql.{Connection, PreparedStatement}
import com.bftv.dt.realtime.model.{FlinkKeyConf, FlinkQuery}
import org.slf4j.LoggerFactory
import scala.collection.mutable.ArrayBuffer

/**
  * 连接mysql执行各类sql操作
  *
  * @author sunliangliang 2019-03-10 https://github.com/sunliangliang9201/tv_realtime_flink
  * @version 1.0
  */
object MysqlDao {

  val logger = LoggerFactory.getLogger(this.getClass)

  val flinkSQL = "select streaming_key, app_name, driver_cores, formator, topics, group_id, table_name, fields, broker_list from realtime_streaming_config where streaming_key = ?"

  val querySQL = "select a.task_key, a.dim_index, a.select_sql, a.insert_table, b.db_name, b.db_host, b.db_user, b.db_port, b.db_pwd from " +
    "realtime_report_config a join wireless_meta.meta_db_info b on a.target_dbkey = b.db_key where a.enable=1 and a.streaming_key=?"

  val tableSQL = "desc %s.%s"

  /**
    * 获取初始化的配置
    * @param ssKey key
    * @return 配置的case 对象
    */
  def getFlinkKeyConf(ssKey: String): FlinkKeyConf ={
    var flinkKeyConf: FlinkKeyConf = null
    var conn: Connection = null
    var ps: PreparedStatement = null

    try{
      conn  = MysqlManager.getMysqlManager.getConnection
      ps = conn.prepareStatement(flinkSQL)
      ps.setNString(1, ssKey)
      val res = ps.executeQuery()
      res.last()
      val rows = res.getRow
      if (rows == 1){
        flinkKeyConf = FlinkKeyConf(
          res.getString("streaming_key"),
          res.getString("app_name"),
          res.getString("driver_cores"),
          res.getString("formator"),
          res.getString("topics"),
          res.getString("group_id"),
          res.getString("table_name"),
          res.getString("fields").split(","),
          res.getString("broker_list")
        )
      }else{
        throw new Exception("Mysql flinkkey key set error ...")
      }
    }catch {
      case e: Exception => logger.error("Find the flinkkeyConf from mysql failed ..., " + e)
    }finally {
      if (null != ps){
        ps.close()
      }
      if (null != conn){
        conn.close()
      }
    }
    flinkKeyConf
  }

  /**
    * 从mysql的realtime report表和meta db info表中获取需要执行的sql以及结果表所需字段，插入表等信息
    * @param flinkKey key
    * @return array[object]
    */
  def getQueryConfig(flinkKey: String): Array[FlinkQuery] ={
    var querys = ArrayBuffer[FlinkQuery]()
    var conn: Connection = null
    var ps: PreparedStatement = null
    try{
      conn  = MysqlManager.getMysqlManager.getConnection
      ps = conn.prepareStatement(querySQL)
      ps.setNString(1, flinkKey)
      val res = ps.executeQuery()
      while (res.next()){
        val flinkQuery = FlinkQuery(
          task_key = res.getString("task_key"),
          res_fields = res.getString("dim_index"),
          select_sql = res.getString("select_sql"),
          insert_table = res.getString("insert_table"),
          db_name = res.getString("db_name"),
          db_host = res.getString("db_host"),
          db_user = res.getString("db_user"),
          db_port = res.getString("db_port"),
          db_pwd = res.getString("db_pwd")
        )
        querys.append(flinkQuery)
      }
      return querys.toArray
    }catch {
      case e: Exception => logger.error("Find the querySQLConfig from mysql failed ..., " + e)
    }finally {
      if (null != ps){
        ps.close()
      }
      if (null != conn){
        conn.close()
      }
    }
    querys.toArray
  }

  def getTableInfo(flinkQuery: FlinkQuery): Array[(String, String)] ={
    var infos = ArrayBuffer[(String, String)]()
    var conn: Connection = null
    var ps: PreparedStatement = null
    try{
      conn  = MysqlManager.getMysqlManager.getConnection
      ps = conn.prepareStatement(tableSQL.format(flinkQuery.db_name, flinkQuery.insert_table))
      val res = ps.executeQuery()
      while (res.next()){
        infos.append((res.getString(1), res.getString(2)))
      }
      return infos.toArray
    }catch {
      case e: Exception => logger.error("Find the tableINFO from mysql failed ..., " + e)
    }finally {
      if (null != ps){
        ps.close()
      }
      if (null != conn){
        conn.close()
      }
    }
    infos.toArray
  }

  def main(args: Array[String]): Unit = {
  }
}
