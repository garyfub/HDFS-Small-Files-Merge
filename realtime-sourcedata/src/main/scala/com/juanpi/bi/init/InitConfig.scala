package com.juanpi.bi.init

import java.util.Properties
import java.io.FileInputStream

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel
import play.api.libs.json.{JsValue, Json}

/**
  * Created by gongzi on 2016/7/8.
  */
class InitConfig {

  private[this] var _brokerList: String = ""

  def brokerList: String = _brokerList

  def brokerList_=(value: String): Unit = {
    _brokerList = value
  }

  var groupId = ""
  var zkQuorum = ""
  var consumerType = ""
  var consumerTime = ""
  var hbaseZk = ""

  var conf = new SparkConf()

  def init() = {
    this.loadProperties()

    conf.set("spark.akka.frameSize", "256")
      .set("spark.kryoserializer.buffer.max", "512m")
      .set("spark.kryoserializer.buffer", "256m")
      .set("spark.scheduler.mode", "FAIR")
      .set("spark.storage.blockManagerSlaveTimeoutMs", "8000000")
      .set("spark.storage.blockManagerHeartBeatMs", "8000000")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.rdd.compress", "true")
      .set("spark.io.compression.codec", "org.apache.spark.io.SnappyCompressionCodec")
      // control default partition number
      .set("spark.streaming.blockInterval", "10000")
      .set("spark.shuffle.manager", "SORT")
      .set("spark.eventLog.overwrite", "true")
  }

  def loadProperties():Unit = {
    val properties = new Properties()
    val path = Thread.currentThread().getContextClassLoader.getResource("conf.properties").getPath //文件要放到resource文件夹下
    properties.load(new FileInputStream(path))
    // 读取键为ddd的数据的值
    brokerList_=(properties.getProperty("brokerList"))
    zkQuorum = properties.getProperty("zkQuorum")
    hbaseZk = properties.getProperty("hbaseZk")
  }

  def getHbaseConf(): Connection = {
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.set("hbase.zookeeper.quorum", zkQuorum)
    hbaseConf.setInt("timeout", 120000)
    //Connection 的创建是个重量级的工作，线程安全，是操作hbase的入口
    ConnectionFactory.createConnection(hbaseConf)
  }

  def iniHive() = {
    // 查询 hive 中的 dim_page 和 dim_event
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
  }

  def initDimPage(sqlContext: HiveContext) =
  {
    val dimPageSql = s"""select page_id,page_exp1, page_exp2, concat_ws(",", url1, url2, url3,regexp1, regexp2, regexp3) as url_pattern
                         | from dw.dim_page
                         | where page_id > 0
                         | and terminal_lvl1_id = 2
                         | and del_flag = 0
                         | order by page_id'""".stripMargin

    val dimPageData = sqlContext.sql(dimPageSql).persist(StorageLevel.MEMORY_AND_DISK)

    val dimPages = dimPageData.map(line => {
      val page_id = line.getAs[Int]("page_id")
      val page_exp1 = line.getAs[String]("page_exp1")
      val page_exp2 = line.getAs[String]("page_exp2")
      // 移动端的 page_exp1+page_exp2 不会为空，但是 url_pattern 为空
      val url_pattern = line.getAs[String]("url_pattern")
      (page_exp1+page_exp2, page_id, url_pattern)
    })

    dimPageData.unpersist(true)

//    dimPages
  }

  def initDimEvent(sqlContext: HiveContext) =
  {
    val dimEventSql = s"""select event_id, event_exp1, event_exp2
                         | from dw.dim_event
                         | where event_id > 0
                         | and terminal_lvl1_id = 2
                         | and del_flag = 0
                         | order by event_id'""".stripMargin

    val dimData = sqlContext.sql(dimEventSql).persist(StorageLevel.MEMORY_AND_DISK)

    val dimEvents = dimData.map(line => {
      val page_id = line.getAs[Int]("page_id")
      val page_exp1 = line.getAs[String]("page_exp1")
      val page_exp2 = line.getAs[String]("page_exp2")
      // 移动端的 page_exp1+page_exp2 不会为空，但是 url_pattern 为空
      val url_pattern = line.getAs[String]("url_pattern")
      (page_exp1+page_exp2, page_id, url_pattern)
    })

    dimData.unpersist(true)

//    dimEvents
  }
}

object InitConfig {
  def main(args: Array[String]) {
//    val ic = new InitConfig()
//    ic.loadProperties
//    println(ic.brokerList)
//
//    println(ic.brokerList, ic.consumerTime, ic.groupId, ic.hbaseZk, ic.zkQuorum)
  }
}
