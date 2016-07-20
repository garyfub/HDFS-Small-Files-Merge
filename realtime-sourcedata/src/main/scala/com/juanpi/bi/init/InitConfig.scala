package com.juanpi.bi.init

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Table}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel
import com.typesafe.config.ConfigFactory
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
/**
  * 默认情况下 Scala 使用不可变 Map。如果你需要使用可变集合，你需要显式的引入 import scala.collection.mutable.Map 类
  * 参见 ：https://wizardforcel.gitbooks.io/w3school-scala/content/17.html
   */
import scala.collection.mutable
import scala.reflect.BeanProperty

/**
  * Created by gongzi on 2016/7/8.
  */
class InitConfig() {

  @BeanProperty var spconf: SparkConf = _
  @BeanProperty var AppName: String = _
  @BeanProperty var ssc: StreamingContext = _
  @BeanProperty var duration: Duration = _

//  var hbasePort = ""
  var zkQuorum = ""
  @BeanProperty var hbase_family: String = _

  @BeanProperty var dimPages = new mutable.HashMap[String, (Int, Int, String, Int)]
  @BeanProperty var dimEvents = new mutable.HashMap[String, Int]
  @BeanProperty var ticks_history: None.type = None
  val table_ticks_history = TableName.valueOf("utm_history")

  def initDimPageEvent() = {
    // 查询 hive 中的 dim_page 和 dim_event
    val sqlContext: HiveContext = new HiveContext(this.getSsc().sparkContext)

    initDimPage(sqlContext)
    initDimEvent(sqlContext)
  }

  // 得到初始化的 StreamingContext
   private def setStreamingContext() = {
    this.setSsc(new StreamingContext(this.getSpconf(), this.getDuration()))
  }

  // 初始化 SparkConf 公共参数
  private def initSparkConfig(appName:String): Unit = {
    val conf = new SparkConf().set("spark.akka.frameSize", "256")
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
    this.setSpconf(conf)
  }

  private def loadProperties():Unit = {
    val config = ConfigFactory.load("hbase.conf")
    zkQuorum = config.getString("hbaseConf.zkQuorum")
    this.setHbase_family(config.getString("hbaseConf.hbase_family"))
  }

  private def getHbaseConf(): Connection = {
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", zkQuorum)
    hbaseConf.setInt("timeout", 120000)
    //Connection 的创建是个重量级的工作，线程安全，是操作hbase的入口
    ConnectionFactory.createConnection(hbaseConf)
  }

  private def initDimPage(sqlContext: HiveContext) =
  {
    var dimPages = new mutable.HashMap[String, (Int, Int, String, Int)]
    val dimPageSql = s"""select page_id,page_exp1, page_exp2, page_type_id, page_value, page_level_id, concat_ws(",", url1, url2, url3,regexp1, regexp2, regexp3) as url_pattern
                         | from dw.dim_page
                         | where page_id > 0
                         | and terminal_lvl1_id = 2
                         | and del_flag = 0
                         | order by page_id""".stripMargin

    val dimPageData = sqlContext.sql(dimPageSql).persist(StorageLevel.MEMORY_AND_DISK)

    dimPageData.map(line => {
      val page_id: Int = line.getAs[Int]("page_id")
      val page_exp1 = line.getAs[String]("page_exp1")
      val page_exp2 = line.getAs[String]("page_exp2")
      val page_value = line.getAs[String]("page_value")
      val page_type_id = line.getAs[Int]("page_type_id")
      val page_level_id = line.getAs[Int]("page_level_id")

      val key = page_exp1 + page_exp2
      (page_id, page_type_id, page_value, page_level_id,key)
    }).collect().foreach( items => {
      val page_id: Int = items._1
      val page_type_id = items._2
      val page_value = items._3
      val page_level_id = items._4
      val key = items._5
      dimPages += ( key -> (page_id, page_type_id, page_value, page_level_id))
    })

    this.setDimPages(dimPages)

    dimPageData.unpersist(true)
  }

  private def initDimEvent(sqlContext: HiveContext) =
  {
    val dimEventSql = s"""select event_id, event_exp1, event_exp2
                         | from dw.dim_event
                         | where event_id > 0
                         | and terminal_lvl1_id = 2
                         | and del_flag = 0
                         | order by event_id""".stripMargin

    val dimData = sqlContext.sql(dimEventSql).persist(StorageLevel.MEMORY_AND_DISK)

    dimData.map(line => {
      val event_id = line.getAs[Int]("event_id")
      val event_exp1 = line.getAs[String]("event_exp1")
      val event_exp2 = line.getAs[String]("event_exp2")

      val key = event_exp1 + event_exp2
      (event_id, key)
    }).collect().foreach( items => {
      val event_id: Int = items._1
      val key = items._2
      this.dimEvents += ( key -> event_id)
    })

    dimData.unpersist(true)
  }
}

object InitConfig {

  // 主构造器
  val ic = new InitConfig()
  var DIMPAGE = new mutable.HashMap[String, (Int, Int, String, Int)]

  def initParam(appName: String, interval: Int) = {
    // 根据 topic 设置 appName
    ic.setAppName(appName)

    // 初始化 apark 超时时间, spark.mystreaming.batch.interval
    ic.setDuration(Seconds(interval))

    // 初始化 SparkConfig
    ic.initSparkConfig(ic.getAppName)

    ic.setStreamingContext()

    // load 配置文件
    ic.loadProperties()

    // 初始化 page and event
    ic.initDimPageEvent()

    DIMPAGE = ic.getDimPages()
  }

  def getStreamingContext(): StreamingContext = {
    ic.getSsc()
  }

  def getDimPages_(): mutable.HashMap[String, (Int, Int, String, Int)] =
  {
    println("=== test getDimPages_ ===")
    for((k, v) <- ic.getDimPages)
    {
      println("======getDimPages_=====>> k:" + k, "# v:" + v)
    }
    ic.getDimPages
  }

  def getDimEvents_(): mutable.HashMap[String, Int] =
  {
    ic.getDimEvents
  }

  def getHbaseFamily =
  {
    ic.getHbase_family
  }

  // hbase 创建连接
  def initTicksHistory(): Table =
  {
    ic.getHbaseConf().getTable(ic.table_ticks_history)
  }

  def main(args: Array[String]) {
//    val dimPages_test = new mutable.HashMap[String, (Int, Int, String, Int)]
//    dimPages_test += ("page_taball" -> (219,10,"最新折扣",1))
//    println(dimPages_test.get("page_taball"))
  }
}
