package com.juanpi.bi.streaming

import java.io.Serializable

import com.juanpi.bi.bean.{Event, Page, PageAndEvent, User}
import com.juanpi.bi.init.InitConfig
import com.juanpi.bi.transformer._
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, _}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaManager

import scala.collection.mutable

class KafkaConsumer(topic: String,
                    dimPage: mutable.HashMap[String, (Int, Int, String, Int)],
                    dimEvent: mutable.HashMap[String, (Int, Int, Int)],
                    fCate: mutable.HashMap[String, String],
                    dimH5EVENT: mutable.HashMap[String, (Int, Int, Int)],
                    zkQuorum: String)
  extends Logging with Serializable {
  import KafkaConsumer._

  /**
    * 解析 app 端原生页面点击数据
    * event 过滤 collect_api_responsetime
    * page 和 event 都需要过滤 gu_id 为空的数据，需要过滤 site_id 不为（2, 3）的数据
    *
    * @param dataDStream
    * @param ssc
    * @param km
    */
  def eventProcess(dataDStream: DStream[((Long, Long), String)],
                   ssc: StreamingContext,
                   km: KafkaManager) = {
    // event 中直接顾虑掉 activityname = "collect_api_responsetime" 的数据
    // 数据块中的每一条记录需要处理
    val sourceLog = dataDStream.persist(StorageLevel.MEMORY_AND_DISK_SER)
    val data = sourceLog.map(_._2.replaceAll("(\0|\r|\n)", ""))
      .filter(eventParser.filterFunc)
      .map(msg => parseMBEventMessage(msg))
      .filter(_._1.nonEmpty)

    data.foreachRDD((rdd, time) =>
    {
      val mills = time.milliseconds
      // 保存数据至hdfs
      rdd.map(v => ((v._1, mills), v._3))
        .repartition(1)
        .saveAsHadoopFile(Config.baseDir + "/" + topic,
          classOf[String],
          classOf[String],
          classOf[RDDMultipleTextOutputFormat])
    })

    // 更新kafka offset
    sourceLog.foreachRDD { rdd =>
      km.updateOffsets(rdd)
    }
  }

  /**
    * 解析 pageinfo
    * @param dataDStream
    * @param ssc
    * @param km
    */
  def pageProcess(dataDStream: DStream[((Long, Long), String)],
                  ssc: StreamingContext,
                  km: KafkaManager) = {

    val data = dataDStream.map(_._2.replaceAll("(\0|\r|\n)", ""))
        .map(msg => parseMBPageMessage(msg))
        .filter(_._1.nonEmpty)

     data.foreachRDD((rdd, time) => {

       val mills = time.milliseconds
        //  需要从 hbase 查 utm 和 gu_id 的值，存在就取出来，否则写 hbase
        val newRdd = rdd.map(record => {
          val (user: User, pageAndEvent: PageAndEvent, page: Page, event: Event) = record._3

          val res_str =  pageAndEventParser.combineTuple(user, pageAndEvent, page, event).map(x=> x match {
            case z if z == null => "\\N"
            case y if y == "" || y.toString.isEmpty => "\\N"
            case _ => x
          }).mkString("\001")
          ((record._1, mills), res_str)
        })

        // 保存数据至hdfs: /user/hadoop/gongzi/dw_real_for_path_list/mb_pageinfo_hash2/
        // /user/hadoop/gongzi/dw_real_for_path_list/mb_pageinfo_hash2/date=2016-08-28/gu_hash=0
        newRdd.repartition(1)
          .saveAsHadoopFile(Config.baseDir + "/" + topic,
            classOf[String],
            classOf[String],
            classOf[RDDMultipleTextOutputFormat])
      })

    // 更新kafka offset
    dataDStream.foreachRDD { rdd =>
      km.updateOffsets(rdd)
    }
  }

  /**
    * 解析 event
    * event 过滤 collect_api_responsetime
    * page 和 event 都需要过滤 gu_id 为空的数据，需要过滤 site_id 不为（2, 3）的数据
    *
    * @param dataDStream
    * @param ssc
    * @param km
    */
  def h5EventProcess(dataDStream: DStream[((Long, Long), String)],
                     ssc: StreamingContext,
                     km: KafkaManager) = {

    val sourceLog = dataDStream.persist(StorageLevel.MEMORY_AND_DISK_SER)
    // event 中直接顾虑掉 activityname = "collect_api_responsetime" 的数据
    val data = sourceLog.map(_._2.replaceAll("(\0|\r|\n)", ""))
      .map(msg => parseH5Message(msg))
      .filter(_._1.nonEmpty)

    // 解析后的数据写HDFS
    data.foreachRDD((rdd, time) =>
    {
      val mills = time.milliseconds
      // 保存数据至hdfs
      rdd.map(v => ((v._1, mills), v._3))
        .repartition(1)
        .saveAsHadoopFile(Config.baseDir + "/" + topic,
          classOf[String],
          classOf[String],
          classOf[RDDMultipleTextOutputFormat])
    })

    // 更新kafka offset
    sourceLog.foreachRDD { rdd =>
      km.updateOffsets(rdd)
    }
  }

  def h5PageProcess(dataDStream: DStream[((Long, Long), String)],
                     ssc: StreamingContext,
                     km: KafkaManager) = {

    val sourceLog = dataDStream.persist(StorageLevel.MEMORY_AND_DISK_SER)
    // event 中直接顾虑掉 activityname = "collect_api_responsetime" 的数据
    val data = sourceLog.map(_._2.replaceAll("(\0|\r|\n)", ""))
      .map(msg => parseH5Page(msg))
      .filter(_._1.nonEmpty)

    // 解析后的数据写HDFS
    data.foreachRDD((rdd, time) =>
    {
      val mills = time.milliseconds
      // 保存数据至hdfs
      rdd.map(v => ((v._1, mills), v._3))
        .repartition(1)
        .saveAsHadoopFile(Config.baseDir + "/" + topic,
          classOf[String],
          classOf[String],
          classOf[RDDMultipleTextOutputFormat])
    })

    // 更新kafka offset
    sourceLog.foreachRDD { rdd =>
      km.updateOffsets(rdd)
    }
  }

  def parseH5Page(message:String):(String, String, Any) = {
    val h5LogTransformer = new H5PageTransformer()
    h5LogTransformer.logParser(message, dimPage)
  }

  def parseH5Message(message:String):(String, String, Any) = {
    val h5LogTransformer = new H5EventTransformer()
    h5LogTransformer.logParser(message, dimPage, dimH5EVENT)
  }

  def parseMBEventMessage(message:String):(String, String, Any) = {
    val mbEventTransformer = new MbEventTransformer()
    mbEventTransformer.logParser(message, dimPage, dimEvent, fCate)
  }

  def parseMBPageMessage(message:String):(String, String, Any) = {
    val pageTransformer = new PageinfoTransformer()
    pageTransformer.logParser(message, dimPage, dimEvent, fCate)
  }
}

object HBaseHandler {
  val HbaseFamily = "dw"
  var conn: Connection = null

  /**
    * @param zkQuorum
    * @return
    */
  private def getHBaseConnection(zkQuorum: String): Connection = {
    // TODO 需要优化
    if(conn == null) {
      val hbaseConf = HBaseConfiguration.create()
      hbaseConf.set("hbase.zookeeper.quorum", zkQuorum)
      hbaseConf.setInt("timeout", 120000)

      // Connection 的创建是个重量级的工作，线程安全，是操作hbase的入口
      conn = ConnectionFactory.createConnection(hbaseConf)
    }
    conn
  }

  /**
    * 查hbase 从 ticks_history 中查找 ticks 存在的记录
    *
    * @param zkQuorum
    * @param id
    * @return
    */
  def getGuIdUtmInitDate(zkQuorum: String, id: String): (String, String) = {

    val table_ticks_history = TableName.valueOf("ticks_history")
    val conn = getHBaseConnection(zkQuorum)
    val ticks_history = conn.getTable(table_ticks_history)

    var utm = ""
    var gu_create_time = ""
    val key = new Get(Bytes.toBytes(id))
    println("=======> ticks_history.get:" + key)
    val ticks_res = ticks_history.get(key)

    if (!ticks_res.isEmpty) {
      utm = Bytes.toString(ticks_res.getValue(HbaseFamily.getBytes, "utm".getBytes))
      gu_create_time = Bytes.toString(ticks_res.getValue(HbaseFamily.getBytes, "gu_create_time".getBytes))
      (utm, gu_create_time)
    }
    else {
      // 如果不存在就写入 hbase
      val p = new Put(id.getBytes)
      // 为put操作指定 column 和 value （以前的 put.add 方法被弃用了）
      p.addColumn(HbaseFamily.getBytes, "utm".getBytes, utm.getBytes)
      p.addColumn(HbaseFamily.getBytes, "gu_create_time".getBytes, gu_create_time.getBytes)
      //提交
      ticks_history.put(p)
      (utm, gu_create_time)
    }
  }
}

object KafkaConsumer{

  class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {
    override def generateActualKey(key: Any, value: Any): Any = {
      NullWritable.get()
    }

    override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String = {
      // date=2016-08-28/gu_hash=0
      val keyAndTime = key.asInstanceOf[(String, Long)]
      val realKey = keyAndTime._1
      val timestamp = keyAndTime._2
      realKey + "/logs/part_" + timestamp
    }
  }

  /**
    * "zkQuorum":"GZ-JSQ-JP-BI-KAFKA-001.jp:2181,GZ-JSQ-JP-BI-KAFKA-002.jp:2181,GZ-JSQ-JP-BI-KAFKA-003.jp:2181,GZ-JSQ-JP-BI-KAFKA-004.jp:2181,GZ-JSQ-JP-BI-KAFKA-005.jp:2181" "brokerList":"kafka-broker-000.jp:9082,kafka-broker-001.jp:9083,kafka-broker-002.jp:9084,kafka-broker-003.jp:9085,kafka-broker-004.jp:9086,kafka-broker-005.jp:9087,kafka-broker-006.jp:9092,kafka-broker-007.jp:9093,kafka-broker-008.jp:9094,kafka-broker-009.jp:9095,kafka-broker-010.jp:9096,kafka-broker-011.jp:9097" "topic":"mb_pageinfo_hash2" "groupId":"pageinfo_direct_dw" "consumerType":1 "consumerTime":5
    *
    * @param args
    */
  def main(args: Array[String]) {

    println("======>> 2017-01-09 com.juanpi.bi.streaming.KafkaConsumer 开始运行，参数个数：" + args.length)

    // 判断传入的参数
    if (args.length < 3) {
      System.err.println(s"""
                            |Usage: KafkaConsumerOffset <zkQuorum> <brokers> <topic> <groupId> <consumerType> <consumerTime>
                            | 192.168.16.50:8081 192.168.16.50:8081 pageinfo pageinfo_direct_dw 1 60
                            |  <zkQuorum> zookeeper address to save kafka consumer offsets
                            |  <brokers> is a list of one or more Kafka brokers
                            |  <topic> topic name
                            |  <table> table name
                            |  <groupId> consumer groupId name
                            |  <consumerType> consumer type 1-every batch offset save 2-recove from 5 minutes save datetime
                            |  <consumerTime> recoved time
                            |  <maxRecords> max number of records per second
        """.stripMargin)
      System.exit(1)
    }

    // 初始化必要的参数
    var (zkQuorum, brokerList, topic, groupId, consumerType, consumerTime, maxRecords) = ("", "", "", "", "1", "60", "100")

    // 解析传入的参数
    println("com.juanpi.bi.streaming.KafkaConsumer 开始运行。。。。。。传入参数如下：")
    args.foreach(
      arg => {
        val k = arg.split("=")(0)
        val v = arg.split("=")(1)
        k match {
          case "zkQuorum" => zkQuorum = v
          case "brokerList" => brokerList = v
          case "topic" => topic = v
          case "groupId" => groupId = v
          case "consumerType" => consumerType = v
          case "consumerTime" => consumerTime = v
          case "maxRecords" => maxRecords = v
        }
      }
    )

    if(!Config.kafkaTopicMap.contains(topic)){
      System.err.println(s"没有找到表:${topic}配置信息")
      System.exit(1)
    }

    // 初始化 SparkConfig StreamingContext HiveContext
    val ic = InitConfig
    // 时间间隔采用的是写死的，目前是 60 s
    ic.initParam(groupId, Config.interval, maxRecords)
    val ssc = ic.getStreamingContext()

    // 连接Kafka参数设置
    val kafkaParams : Map[String, String] = Map(
      "metadata.broker.list" -> brokerList,
      "auto.offset.reset" -> "largest",
      "group.id" -> groupId)

    // init beginning offset number, it could consumer which data with config file
    val km = new KafkaManager(kafkaParams, zkQuorum)

    /**
      * consumerType = "2", 用于当解析数据出错后，手动刷数据之用，需要手动指定offset
      * ！运行之前需要跟架构沟通
     */
    if (consumerType.equals("2")) {
      km.setConfigOffset(Set(topic), groupId, consumerTime, ssc)
    }

    val message = km.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set(topic))

    // page 和 event 分开解析
    if(topic.equals("mb_pageinfo_hash2")) {
      val DimPage = InitConfig.initMBDim()._1
      val DimEvent = InitConfig.initMBDim()._2
      val DimFrontCate = InitConfig.initMBDim()._3
      val consumer = new KafkaConsumer(groupId, DimPage, DimEvent, DimFrontCate, null, zkQuorum)
      consumer.pageProcess(message, ssc, km)
    } else if(topic.equals("mb_event_hash2")) {
      val DimPage = InitConfig.initMBDim()._1
      val DimEvent = InitConfig.initMBDim()._2
      val DimFrontCate = InitConfig.initMBDim()._3
      val consumer = new KafkaConsumer(groupId, DimPage, DimEvent, DimFrontCate, null, zkQuorum)
      consumer.eventProcess(message, ssc, km)
    } else if(topic.equals("pc_events_hash3")) {
      val DimH5Page = InitConfig.initH5Dim()._1
      val DimH5Event = InitConfig.initH5Dim()._2
      val consumer = new KafkaConsumer(groupId, DimH5Page, null, null, DimH5Event, zkQuorum)
      consumer.h5EventProcess(message, ssc, km)
    } else if(topic.equals("jp_hash3")) {
      val DimH5Page = InitConfig.initH5Dim()._1
      val consumer = new KafkaConsumer(groupId, DimH5Page, null, null, null, zkQuorum)
      consumer.h5PageProcess(message, ssc, km)
    }
    else {
      println("请指定需要解析的kafka Topic！！")
      System.exit(1)
    }

    ssc.start()
    ssc.awaitTermination()
  }
}

