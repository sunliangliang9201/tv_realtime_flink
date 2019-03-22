package com.bftv.dt.realtime.main

import java.sql.{Date, Timestamp}
import java.util.{Properties, TimeZone}

import com.bftv.dt.realtime.format.LogFormator
import com.bftv.dt.realtime.model.{FlinkQuery, JDBCSinkFactory, MyAssigner, MyMapFunction}
import com.bftv.dt.realtime.storage.MysqlDao
import com.bftv.dt.realtime.utils.Constant
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.slf4j.LoggerFactory

import scala.collection.mutable.Set

/**
  * 主类入口：初始化各种配置，并创建关键对象；包含主要逻辑
  *
  * @author sunliangliang 2019-03-10 https://github.com/sunliangliang9201/tv_realtime_flink
  * @version 1.0
  */
object TvRealTimeMain2 {

  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val flinkKey = "TvDisplay2"
    val flinkKeyConf = MysqlDao.getFlinkKeyConf(flinkKey)
    if (null == flinkKeyConf) {
      logger.error("No flinkKey config im mysql ...")
      System.exit(1)
    }
    logger.info("Success load the flinkKey config from mysql !")
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setMaxParallelism(128)
    env.enableCheckpointing(30000)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(15)))
    env.registerCachedFile("e:/ip_area_isp.txt", "ips")
    //env.registerCachedFile("hdfs://cluster/test/sunliangliang/ip_area_isp.txt", "ips")
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(5000)
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
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
    //kafkaConsumer.setStartFromGroupOffsets()

    val ds = env.addSource(kafkaConsumer).map(new MyMapFunction(logFormator, flinkKeyConf.fields)).filter( bean => {
      null != bean && bean.value != "-"
    })
    val ds2 = ds.assignTimestampsAndWatermarks(new MyAssigner())
    //schema (String, String...., Timestamp)
    tableEnv.registerDataStream("tv_heart", ds2, 'country, 'province, 'city, 'isp, 'appkey, 'ltype, 'uid, 'imei, 'userid, 'mac, 'apptoken, 'ver, 'mtype, 'version, 'androidid, 'unet, 'mos, 'itime, 'uuid, 'gid, 'value, 'rowtime.rowtime)


//    tableEnv.sqlQuery("select HOP_END(rowtime, INTERVAL '1' minute, INTERVAL '24' hour) as end_window, cast(DATE_FORMAT(rowtime, '%Y-%m-%d') as Date) as dt, count(uuid) from tv_heart group by cast(DATE_FORMAT(rowtime, '%Y-%m-%d') as Date), HOP(rowtime, INTERVAL '1' minute, INTERVAL '24' hour)").toAppendStream[(Timestamp, Date, Long)](queryConfig).print()
    tableEnv.sqlQuery("select HOP_END(rowtime, INTERVAL '1' minute, INTERVAL '2' minute) as end_window, cast(DATE_FORMAT(rowtime, '%Y-%m-%d') as Date) as dt, count(uuid) from tv_heart group by cast(DATE_FORMAT(rowtime, '%MM') as Date), HOP(rowtime, INTERVAL '1' minute, INTERVAL '2' minute)").toAppendStream[(Timestamp, Date, Long)](queryConfig).print()

//    val jdbcSink = JDBCAppendTableSink.builder()
//      .setDrivername("com.mysql.jdbc.Driver")
//      .setDBUrl("jdbc:mysql://103.26.158.76:3306/bftv_realtime")
//      .setQuery("insert into test(end_window) values(?)")
//      .setUsername("dtadmin")
//      .setPassword("Dtadmin123!@#")
//      .setParameterTypes(createTypeInformation[Timestamp])
//      .build()
//    tableEnv.registerTableSink("sink1",Array("end_window") , Array(createTypeInformation[Timestamp]), jdbcSink)


    env.execute(flinkKeyConf.appName)
  }
}