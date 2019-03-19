package com.bftv.dt.realtime.main

import java.util.{Properties, TimeZone}

import com.bftv.dt.realtime.format.LogFormator
import com.bftv.dt.realtime.model.{FlinkQuery, JDBCSinkFactory, MyAssigner, MyMapFunction}
import com.bftv.dt.realtime.storage.MysqlDao
import com.bftv.dt.realtime.utils.Constant
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.table.api.TableEnvironment
import org.slf4j.LoggerFactory
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._

/**
  * 主类入口：初始化各种配置，并创建关键对象；包含主要逻辑
  *
  * @author sunliangliang 2019-03-10 https://github.com/sunliangliang9201/tv_realtime_flink
  * @version 1.0
  */
object TvRealTimeMain {

  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    //val flinkKey = args(0)
    val flinkKey = "TvDisplay"
    val flinkKeyConf = MysqlDao.getFlinkKeyConf(flinkKey)
    if (null == flinkKeyConf) {
      logger.error("No flinkKey config im mysql ...")
      System.exit(1)
    }
    logger.info("Success load the flinkKey config from mysql !")

    //env config
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setMaxParallelism(128)
    env.enableCheckpointing(10000)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(15)))
    //env.registerCachedFile("e:/ip_area_isp.txt", "ips")
    env.registerCachedFile("hdfs://cluster/test/sunliangliang/ip_area_isp.txt", "ips")
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(5000)
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    //env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getConfig.setUseSnapshotCompression(true)

    //table&query env config, 注意：不要轻易指定变量的父类类型，吃了大亏了已经！！！
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    val tableConfig = tableEnv.getConfig
    tableConfig.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
    val queryConfig = tableEnv.queryConfig
    queryConfig.withIdleStateRetentionTime(Time.hours(12), Time.hours(24))
    //引入kafka数据流，两种方式，一种是使用stream API，另一种是使用table API，为了更容易理解先使用第一种方式
    val prop: Properties = new Properties()
    prop.setProperty("bootstrap.servers", flinkKeyConf.brolerList)
    prop.setProperty("group.id", flinkKeyConf.groupID)
    prop.setProperty("flink.partition-discovery.interval-millis", "30000")
    val topicList = new java.util.ArrayList[String]()
    flinkKeyConf.topics.split(",").map(topicList.add(_))
    val logFormator = Class.forName(Constant.FORMATOR_PACACKE_PREFIX + flinkKeyConf.formator).newInstance().asInstanceOf[LogFormator]
    val kafkaConsumer = new FlinkKafkaConsumer09[String](topicList, new SimpleStringSchema, prop)
    kafkaConsumer.setStartFromLatest()
    kafkaConsumer.setStartFromGroupOffsets()

    val ds = env.addSource(kafkaConsumer).map(new MyMapFunction(logFormator, flinkKeyConf.fields)).assignTimestampsAndWatermarks(new MyAssigner())
    tableEnv.registerDataStream("tv_heart", ds, 'country, 'province, 'city, 'isp, 'appkey, 'ltype, 'uid, 'imei, 'userid, 'mac, 'apptoken, 'ver, 'mtype, 'version, 'androidid, 'unet, 'mos, 'itime, 'uuid, 'gid, 'sn, 'plt_ver, 'package_name, 'pid, 'lau_ver, 'plt, 'softid, 'page_title, 'ip, 'value, 'rowtime.rowtime)
    ds.setParallelism(1)
    //接下来从mysql获取需要执行的sql以及结果表
    val querys: Array[FlinkQuery] = MysqlDao.getQueryConfig(flinkKey)
    for (i <- querys) {
      val sinkConf = JDBCSinkFactory.getJDBCSink(i)
      tableEnv.registerTableSink(i.task_key, sinkConf._2.map(_._1), sinkConf._2.map(_._2), sinkConf._1)
      tableEnv.sqlQuery(i.select_sql).insertInto(i.task_key, queryConfig)
      println(1)
    }

    //测试
    //    val jdbcSink = JDBCAppendTableSink.builder()
    //      .setDrivername("com.mysql.cj.jdbc.Driver")
    //      .setDBUrl("jdbc:mysql://103.26.158.76:3306/bftv")
    //      .setQuery("insert into bftv.tv_display_window_active(end_window,counts) values(?,?)")
    //      .setUsername("dtadmin")
    //      .setPassword("Dtadmin123!@#")
    //      .setParameterTypes(createTypeInformation[Timestamp], createTypeInformation[Long])
    //      .build()
    //    tableEnv.registerTableSink(
    //      "active",
    //      Array[String]("end_window", "counts"),
    //      Array[TypeInformation[_]](createTypeInformation[Timestamp], createTypeInformation[Long]),
    //      jdbcSink
    //    )
    //    tableEnv.sqlQuery("select HOP_END(rowtime, INTERVAL '5' second, INTERVAL '20' second) as end_window, count(distinct(sn)) as counts from tv_heart group by HOP(rowtime, INTERVAL '5' second, INTERVAL '20' second)").insertInto("active")
    //
    //    val jdbcSink2 = JDBCAppendTableSink.builder()
    //      .setDrivername("com.mysql.cj.jdbc.Driver")
    //      .setDBUrl("jdbc:mysql://103.26.158.76:3306/bftv")
    //      .setQuery("insert into bftv.tv_display_window_position(end_window,country,province,city,counts) values(?,?,?,?,?)")
    //      .setUsername("dtadmin")
    //      .setPassword("Dtadmin123!@#")
    //      .setParameterTypes(createTypeInformation[Timestamp], createTypeInformation[String], createTypeInformation[String], createTypeInformation[String], createTypeInformation[Long])
    //      .build()
    //    tableEnv.registerTableSink(
    //      "position",
    //      Array[String]("end_window", "country", "province", "city", "counts"),
    //      Array[TypeInformation[_]](createTypeInformation[Timestamp], createTypeInformation[String], createTypeInformation[String], createTypeInformation[String], createTypeInformation[Long]),
    //      jdbcSink2
    //    )
    //    tableEnv.sqlQuery("select HOP_END(rowtime, INTERVAL '5' second, INTERVAL '20' second) as end_window, country, province, city, count(distinct(sn)) as counts from tv_heart group by HOP(rowtime, INTERVAL '5' second, INTERVAL '20' second), country, province, city").insertInto("position")
    //tableEnv.sqlQuery("select country, rowtime from tv_heart").toAppendStream[(String, Timestamp)].print()
    env.execute()
  }
}
