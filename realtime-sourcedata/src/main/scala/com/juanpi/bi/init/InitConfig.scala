package com.juanpi.bi.init

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
/**
  * 默认情况下 Scala 使用不可变 Map。如果你需要使用可变集合，你需要显式的引入 import scala.collection.mutable.Map 类
  * 参见 ：https://wizardforcel.gitbooks.io/w3school-scala/content/17.html
   */
import scala.collection.mutable
import scala.beans.BeanProperty

/**
  * Created by gongzi on 2016/7/8.
  */
class InitConfig() {
  @BeanProperty var spconf: SparkConf = _
  @BeanProperty var ssc: StreamingContext = _
  @BeanProperty var duration: Duration = _

  def initDimTables(): (mutable.HashMap[String, (Int, Int, String, Int)],
    mutable.HashMap[String, (Int, Int, Int)],
    mutable.HashMap[String, String]) = {

    // 查询 hive 中的 dim_page 和 dim_event
    val sqlContext: HiveContext = new HiveContext(this.getSsc().sparkContext)
    val dp: mutable.HashMap[String, (Int, Int, String, Int)] = initDimPage(sqlContext)
    val de: mutable.HashMap[String, (Int, Int, Int)] = initDimEvent(sqlContext)
    val fCate: mutable.HashMap[String, String] = initDimFrontCate(sqlContext)

    (dp, de, fCate)
  }

  /**
    * 解析h5需用到的 dim_page 和 dim_event
    * @return
    */
  def initDimH5Tables(): (mutable.HashMap[String, (Int, Int, String, Int)],
    mutable.HashMap[String, (Int, Int, Int)]) = {

    // 查询 hive 中的 dim_page 和 dim_event
    val sqlContext: HiveContext = new HiveContext(this.getSsc().sparkContext)
    val dp: mutable.HashMap[String, (Int, Int, String, Int)] = initDimH5Page(sqlContext)
    val dH5 = initDimH5Event(sqlContext)

    (dp, dH5)
  }

  /**
    * 解析h5需用到的 dim_page 和 dim_event
    * @return
    */
  def initEventDimTables(): (mutable.HashMap[String, (Int, Int, String, Int)],
    mutable.HashMap[String, (Int, Int, Int)]) = {

    // 查询 hive 中的 dim_page 和 dim_event
    val sqlContext: HiveContext = new HiveContext(this.getSsc().sparkContext)
    val dp: mutable.HashMap[String, (Int, Int, String, Int)] = initDimH5Page(sqlContext)
    val dH5 = initDimH5Event(sqlContext)
    (dp, dH5)
  }

  /**
    * 得到初始化的 StreamingContext
    */
   private def setStreamingContext() = {
    this.setSsc(new StreamingContext(this.getSpconf(), this.getDuration()))
  }

  /**
    * 初始化 SparkConf 公共参数
    * @param appName
    * @param maxRecords
    */
  private def initSparkConfig(appName:String, maxRecords: String): Unit = {
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
      // max number of records per second
      .set("spark.streaming.kafka.maxRatePerPartition", maxRecords)
    this.setSpconf(conf)
  }

  /**
    * 初始化 dw.dim_page
    * @param sqlContext
    * @return
    */
  def initDimPage(sqlContext: HiveContext): mutable.HashMap[String, (Int, Int, String, Int)] =
  {
    var dimPages = new mutable.HashMap[String, (Int, Int, String, Int)]
    val dimPageSql = s"""select page_id,page_exp1, page_exp2, page_type_id, page_value, page_level_id
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
      (page_id, page_type_id, page_value, page_level_id, key)
    })
      .collect()
      .foreach( items => {
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

  /**
    * 初始化 dw.dim_event
    * @param sqlContext
    * @return
    */
  def initDimEvent(sqlContext: HiveContext): mutable.HashMap[String, (Int, Int, Int)] =
  {
    var dimEvents = new mutable.HashMap[String, (Int, Int, Int)]
    val dimEventSql = s"""select event_id, event_exp1, event_exp2, event_type_id, event_level_id
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
      val event_level_id = line.getAs[Int]("event_level_id")

      val key = event_exp1 + event_exp2
      (event_id, event_type_id, key, event_level_id)
    })
    .collect()
    .foreach( items => {
      val event_id: Int = items._1
      val event_type_id = items._2
      val key = items._3
      val event_level_id = items._4
      dimEvents += (key -> (event_id, event_type_id, event_level_id))
    })

    dimData.unpersist(true)
    dimEvents
  }

  /**
    * 初始化 dw.dim_page
    * @param sqlContext
    * @return
    */
  def initDimH5Page(sqlContext: HiveContext): mutable.HashMap[String, (Int, Int, String, Int)] =
  {
    var dimPages = new mutable.HashMap[String, (Int, Int, String, Int)]
    val dimPageSql = s"""select page_id, page_type_id, page_value, page_level_id
                         | from dw.dim_page
                         | where page_id > 0
                         | and del_flag = 0
                         | order by page_id""".stripMargin

    val dimPageData = sqlContext.sql(dimPageSql).persist(StorageLevel.MEMORY_AND_DISK)

    dimPageData.map(line => {
      val page_id: Int = line.getAs[Int]("page_id")
      val page_value = line.getAs[String]("page_value")
      val page_type_id = line.getAs[Int]("page_type_id")
      val page_level_id = line.getAs[Int]("page_level_id")
      (page_id, page_type_id, page_value, page_level_id)
    })
      .collect()
      .foreach( items => {
        val pageId: Int = items._1
        val pageTypeId = items._2
        val pageValue = items._3
        val pageLevelId = items._4
        dimPages += (pageId.toString -> (pageId, pageTypeId, pageValue, pageLevelId))
      })
    dimPageData.unpersist(true)
    dimPages
  }

  /**
    * 初始化 dw.dim_event
    * @param sqlContext
    * @return
    */
  def initDimH5Event(sqlContext: HiveContext): mutable.HashMap[String, (Int, Int, Int)] =
  {
    var dimEvents = new mutable.HashMap[String, (Int, Int, Int)]
    val dimEventSql = s"""select event_id, event_exp1, event_exp2, event_type_id, event_type_name, event_level_id
                          | from dw.dim_event
                          | where event_id > 0
                          | and terminal_lvl1_id = 1
                          | and del_flag = 0
                          | order by event_id""".stripMargin

    val dimData = sqlContext.sql(dimEventSql).persist(StorageLevel.MEMORY_AND_DISK)

    dimData.map(line => {
      val eventId = line.getAs[Int]("event_id")
      val eventTypeId = line.getAs[Int]("event_type_id")
      val eventExp1 = line.getAs[String]("event_exp1")
      val eventTypeName = line.getAs[String]("event_type_name")
      val eventLevelId =  line.getAs[Int]("event_level_id")

      val key = eventExp1 + ScalaConstants.JoinDelimiter + eventTypeName
      (key, eventId, eventTypeId, eventLevelId)
    })
      .collect()
      .foreach( items => {
        dimEvents += ( items._1 -> (items._2, items._3, items._4))
      })

    dimData.unpersist(true)
    dimEvents
  }

  /**
    * 初始化 dw.dim_front_cate 数据
    * @param sqlContext
    * @return
    */
  def initDimFrontCate(sqlContext: HiveContext): mutable.HashMap[String, String] =
  {
    var dimValues = new mutable.HashMap[String, String]
    val sql = s"""select front_cate_id, page_level_id
                          | from dw.dim_front_cate
                          | order by front_cate_id""".stripMargin

    val dimData = sqlContext.sql(sql).persist(StorageLevel.MEMORY_AND_DISK)

    dimData.map(line => {
      val front_cate_id = line.getAs[Int]("front_cate_id")
      val level_id = line.getAs[Int]("page_level_id")

      val key = front_cate_id.toString
      (key, level_id.toString)
    })
      .collect()
      .foreach( items => {
        val value = items._2
        val key = items._1
        dimValues += (key -> value)
      })

    dimData.unpersist(true)
    dimValues
  }
}


object InitConfig {

  val ic = new InitConfig()

  /**
    * 加载配置文件
    */
  def loadProperties():(String, String) = {
    val con = ConfigFactory.load("config.properties")
    val dataBaseDir = con.getString("dataBaseDir")
    val kafkaTopicIds = con.getString("kafkaTopicIds")
    (dataBaseDir, kafkaTopicIds)
  }

  // 主构造器
  def initH5Dim() = {
    val dp = ic.initDimH5Tables()._1
    val de = ic.initDimH5Tables()._2
    (dp, de)
  }

  def initMBDim() = {
    // 初始化 page and event
    val initDimTables = ic.initDimTables()

    val dp = initDimTables._1
    val de  = initDimTables._2
    val df   = initDimTables._3

    (dp, de, df)
  }

  def initParam(appName: String, interval: Int, maxRecords: String) = {
    // 初始化 apark 超时时间, spark.mystreaming.batch.interval
    ic.setDuration(Seconds(interval))

    // 初始化 SparkConfig
    ic.initSparkConfig(appName, maxRecords)

    ic.setStreamingContext()
  }

  def getStreamingContext(): StreamingContext = {
    ic.getSsc()
  }

  def main(args: Array[String]): Unit = {
    loadProperties
  }
}
