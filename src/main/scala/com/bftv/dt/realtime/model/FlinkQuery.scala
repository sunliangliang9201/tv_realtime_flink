package com.bftv.dt.realtime.model

/**
  * query Object
  *
  * @author sunliangliang 2019-03-10 https://github.com/sunliangliang9201/tv_realtime_flink
  * @version 1.0
  */
case class FlinkQuery (
                        task_key: String,
                        res_fields: String,
                        select_sql: String,
                        insert_table: String,
                        db_name: String,
                        db_host: String,
                        db_user: String,
                        db_port: String,
                        db_pwd: String
                      )
