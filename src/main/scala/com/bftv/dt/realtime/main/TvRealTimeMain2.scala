package com.bftv.dt.realtime.main

import java.sql.Timestamp
import java.util.{Properties, TimeZone}

import com.bftv.dt.realtime.format.LogFormator
import com.bftv.dt.realtime.model._
import com.bftv.dt.realtime.storage.MysqlDao
import com.bftv.dt.realtime.utils.Constant
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.slf4j.LoggerFactory


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
    env.enableCheckpointing(60000)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(15)))
    env.registerCachedFile("e:/ip_area_isp.txt", "ips")
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(30000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
    env.getCheckpointConfig.setFailOnCheckpointingErrors(false)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.getConfig.setUseSnapshotCompression(true)

    val tableEnv = TableEnvironment.getTableEnvironment(env)
    val tableConfig = tableEnv.getConfig
    tableConfig.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
    val queryConfig = tableEnv.queryConfig
    queryConfig.withIdleStateRetentionTime(Time.hours(12), Time.hours(24))

    val prop: Properties = new Properties()
    prop.setProperty("bootstrap.servers", flinkKeyConf.brolerList)
    prop.setProperty("group.id", flinkKeyConf.groupID)
    prop.setProperty("flink.partition-discovery.interval-millis", "30000")
    val topicList = new java.util.ArrayList[String]()
    flinkKeyConf.topics.split(",").map(topicList.add(_))
    val logFormator = Class.forName(Constant.FORMATOR_PACACKE_PREFIX + flinkKeyConf.formator).newInstance().asInstanceOf[LogFormator]
    val kafkaConsumer = new FlinkKafkaConsumer09[String](topicList, new SimpleStringSchema, prop)
    kafkaConsumer.setStartFromLatest()

    val ds = env.addSource(kafkaConsumer).map(new MyMapFunction(logFormator, flinkKeyConf.fields)).filter( bean => {
      null != bean && bean.jsonvalue != "-" && bean.uuid != "-"
    }).assignTimestampsAndWatermarks(new MyAssigner()).setParallelism(4)

    tableEnv.registerDataStream("tv_heart", ds, 'country, 'province, 'city, 'isp, 'appkey, 'ltype, 'uid, 'imei, 'userid, 'mac, 'apptoken, 'ver, 'mtype, 'version, 'androidid, 'unet, 'mos, 'itime, 'uuid, 'gid, 'jsonvalue, 'sn, 'plt_ver, 'package_name, 'pid, 'lau_ver, 'plt, 'softid, 'page_title, 'ip, 'rowtime.rowtime)

    tableEnv.registerFunction("myAggreOne", new MyAggregateFunction)

  tableEnv.sqlQuery("select province from tv_heart").toAppendStream[String](queryConfig).print()
    env.execute(flinkKeyConf.appName)
  }
}