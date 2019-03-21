package com.bftv.dt.realtime.main

import java.util.{Properties, TimeZone}

import com.bftv.dt.realtime.format.LogFormator
import com.bftv.dt.realtime.model.{FlinkQuery, JDBCSinkFactory, MyAssigner, MyMapFunction}
import com.bftv.dt.realtime.storage.MysqlDao
import com.bftv.dt.realtime.utils.Constant
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
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
    val ds2 = ds.assignTimestampsAndWatermarks(new MyAssigner()).setParallelism(1)
    tableEnv.registerDataStream("tv_heart", ds2, 'country, 'province, 'city, 'isp, 'appkey, 'ltype, 'uid, 'imei, 'userid, 'mac, 'apptoken, 'ver, 'mtype, 'version, 'androidid, 'unet, 'mos, 'itime, 'uuid, 'gid, 'value, 'rowtime.rowtime)

    //接下来从mysql获取需要执行的sql以及结果表
//    val querys: Array[FlinkQuery] = MysqlDao.getQueryConfig(flinkKey)
//    for (i <- querys) {
//      val sinkConf = new JDBCSinkFactory().getJDBCSink(i)
//      tableEnv.registerTableSink(i.task_key, sinkConf._2.map(_._1), sinkConf._2.map(_._2), sinkConf._1)
//      val resTable = tableEnv.sqlQuery(i.select_sql)
//      //这里的不为null判断是凭感觉加的，实际上可能不起作用
//      if (null != resTable){
//        resTable.insertInto(i.task_key, queryConfig)
//      }
//    }

    tableEnv.sqlQuery("select count(1) from (select uuid from tv_heart group by uuid) tmp_uuid").toRetractStream[Long](queryConfig).print()
    env.execute(flinkKeyConf.appName)
  }
}