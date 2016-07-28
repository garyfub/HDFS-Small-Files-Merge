package com.juanpi.bi.init

import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel
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
  @BeanProperty var ssc: StreamingContext = _
  @BeanProperty var duration: Duration = _

//  var hbasePort = ""
  var zkQuorum = ""
  @BeanProperty var hbase_family: String = _

  @BeanProperty var ticks_history: None.type = None

  def initDimPageEvent(): (mutable.HashMap[String, (Int, Int, String, Int)], mutable.HashMap[String, (Int, Int)]) = {
    // 查询 hive 中的 dim_page 和 dim_event
    val sqlContext: HiveContext = new HiveContext(this.getSsc().sparkContext)
    val dp: mutable.HashMap[String, (Int, Int, String, Int)] = initDimPage(sqlContext)
    val de: mutable.HashMap[String, (Int, Int)] = initDimEvent(sqlContext)
    (dp, de)
  }

  // 得到初始化的 StreamingContext
   private def setStreamingContext() = {
    this.setSsc(new StreamingContext(this.getSpconf(), this.getDuration()))
  }

  // 初始化 SparkConf 公共参数
  private def initSparkConfig(appName:String): Unit = {
    val conf = new SparkConf().setAppName(appName)
      .set("spark.akka.frameSize", "256")
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

  def initDimPage(sqlContext: HiveContext): mutable.HashMap[String, (Int, Int, String, Int)] =
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

    dimPageData.unpersist(true)
    dimPages
  }

  def initDimEvent(sqlContext: HiveContext): mutable.HashMap[String, (Int, Int)] =
  {
    var dimEvents = new mutable.HashMap[String, (Int, Int)]
    val dimEventSql = s"""select event_id, event_exp1, event_exp2, event_type_id
                         | from dw.dim_event
                         | where event_id > 0
                         | and terminal_lvl1_id = 2
                         | and del_flag = 0
                         | order by event_id""".stripMargin

    val dimData = sqlContext.sql(dimEventSql).persist(StorageLevel.MEMORY_AND_DISK)

    dimData.map(line => {
      val event_id = line.getAs[Int]("event_id")
      val event_type_id = line.getAs[Int]("event_type_id")
      val event_exp1 = line.getAs[String]("event_exp1")
      val event_exp2 = line.getAs[String]("event_exp2")

      val key = event_exp1 + event_exp2
      (event_id, event_type_id, key)
    }).collect().foreach( items => {
      val event_id: Int = items._1
      val event_type_id = items._2
      val key = items._3
      dimEvents += ( key -> (event_id, event_type_id))
    })

    dimData.unpersist(true)

    dimEvents
  }
}

object InitConfig {

  // 主构造器
  val ic = new InitConfig()
  var DIMPAGE = new mutable.HashMap[String, (Int, Int, String, Int)]
  var DIMENT = new mutable.HashMap[String, (Int, Int)]

  def initParam(appName: String, interval: Int) = {
    // 初始化 apark 超时时间, spark.mystreaming.batch.interval
    ic.setDuration(Seconds(interval))

    // 初始化 SparkConfig
    ic.initSparkConfig(appName)

    ic.setStreamingContext()

    // 初始化 page and event
    DIMPAGE = ic.initDimPageEvent()._1
    DIMENT = ic.initDimPageEvent()._2

  }

  def getStreamingContext(): StreamingContext = {
    ic.getSsc()
  }
}
