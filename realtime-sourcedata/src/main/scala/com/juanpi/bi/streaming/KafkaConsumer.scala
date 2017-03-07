package com.juanpi.bi.streaming

import java.io.Serializable

import com.juanpi.bi.bean.{Event, Page, PageAndEvent, User}
import com.juanpi.bi.init.InitConfig
import com.juanpi.bi.sc_utils.DateUtils
import com.juanpi.bi.transformer._
import kafka.serializer.StringDecoder
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.spark.Logging
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaManager

import scala.collection.mutable

class KafkaConsumer(topic: String,
                    dataBaseDir: String,
                    dimPage: mutable.HashMap[String, (Int, Int, String, Int)],
                    dimEvent: mutable.HashMap[String, (Int, Int, Int)],
                    fCate: mutable.HashMap[String, String],
                    dimH5EVENT: mutable.HashMap[String, (Int, Int, Int)],
                    zkQuorum: String)
  extends Logging with Serializable {
  import KafkaConsumer._

  /**
    * 重新消费的话，groupID必定是re开头，否则无法判断是重复消费还是正常的消费
    * 1. 如果是重新消费Kafka，log的有效的时间范围是7天前至今
    * 2. 如果是正常的实时消费，log的有效时间范围是今天
    * @param groupId
    * @return
    */
  def getDateFilter(groupId: String): (String, String) ={
    // 当前日期
    val endDateStr = DateUtils.getDateMinusDays(0)

    val startDateStr = if(groupId.startsWith("re")) {
      // 重新消费的话，groupID必定是re开头
      DateUtils.getDateMinusDays(6)
    } else {
      // 否则就是当下的日期
      endDateStr
    }
    (startDateStr, endDateStr)
  }

  /**
    * 解析 app 端原生页面点击数据
    * event 过滤 collect_api_responsetime
    * page 和 event 都需要过滤 gu_id 为空的数据，需要过滤 site_id 不为（2, 3）的数据
    *
    * @param dataDStream
    * @param ssc
    * @param km
    */
  def eventProcess(groupId: String,
                    dataDStream: DStream[((Long, Long), String)],
                   ssc: StreamingContext,
                   km: KafkaManager) = {
    // event 中直接顾虑掉 activityname = "collect_api_responsetime" 的数据
    // 数据块中的每一条记录需要处理

    val data = dataDStream.map(_._2.replaceAll("(\0|\r|\n)", ""))
      .filter(eventParser.filterFunc)
      .map(msg => parseMBEventMessage(msg, groupId))
      .filter(_._1.nonEmpty)

    data.foreachRDD((rdd, time) =>
    {
      val mills = time.milliseconds
      // 保存数据至hdfs
      rdd.map(v => ((v._1, mills), v._3))
        .repartition(1)
        .saveAsHadoopFile(dataBaseDir + "/" + topic,
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
    * 解析 pageinfo
    * @param dataDStream
    * @param ssc
    * @param km
    */
  def pageProcess(groupId: String,
                  dataDStream: DStream[((Long, Long), String)],
                  ssc: StreamingContext,
                  km: KafkaManager) = {

    val data = dataDStream.map(_._2.replaceAll("(\0|\r|\n)", ""))
        .map(msg => parseMBPageMessage(msg, groupId))
        .filter(_._1.nonEmpty)

    data.foreachRDD((rdd, time) => {

       val mills = time.milliseconds
        //  TODO 需要从 hbase 查 utm 和 gu_id 的值，存在就取出来，否则写 hbase
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
          .saveAsHadoopFile(dataBaseDir + "/" + topic,
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
  def h5EventProcess(groupId: String,
                     dataDStream: DStream[((Long, Long), String)],
                     ssc: StreamingContext,
                     km: KafkaManager) = {

    // event 中直接顾虑掉 activityname = "collect_api_responsetime" 的数据
    val data = dataDStream.map(_._2.replaceAll("(\0|\r|\n)", ""))
      .map(msg => parseH5Event(msg, groupId))
      .filter(_._1.nonEmpty)

    // 解析后的数据写HDFS
    data.foreachRDD((rdd, time) =>
    {
      val mills = time.milliseconds
      // 保存数据至hdfs
      rdd.map(v => ((v._1, mills), v._3))
        .repartition(1)
        .saveAsHadoopFile(dataBaseDir + "/" + topic,
          classOf[String],
          classOf[String],
          classOf[RDDMultipleTextOutputFormat])
    })

    // 更新kafka offset
    dataDStream.foreachRDD { rdd =>
      km.updateOffsets(rdd)
    }
  }

  def h5PageProcess(groupId: String,
                    dataDStream: DStream[((Long, Long), String)],
                    ssc: StreamingContext,
                    km: KafkaManager) = {

    // event 中直接顾虑掉 activityname = "collect_api_responsetime" 的数据
    val data = dataDStream.map(_._2.replaceAll("(\0|\r|\n)", ""))
      .map(msg => parseH5Page(msg, groupId))
      .filter(_._1.nonEmpty)

    // 解析后的数据写HDFS
    data.foreachRDD((rdd, time) =>
    {
      val mills = time.milliseconds
      // 保存数据至hdfs
      rdd.map(v => ((v._1, mills), v._3))
        .repartition(1)
        .saveAsHadoopFile(dataBaseDir + "/" + topic,
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
    * 解析app端埋点点击数据
    * @param message
    * @param groupId
    * @return
    */
  def parseMBEventMessage(message:String, groupId: String):(String, String, Any) = {
    val mbEventTransformer = new MbEventTransformer()
    val (startDateStr, endDateStr) = getDateFilter(groupId)
    println(s"=======>>正确的数据范围${startDateStr} ~ ${endDateStr}")
    mbEventTransformer.logParser(message, dimPage, dimEvent, fCate, startDateStr, endDateStr)
  }

  /**
    * 解析app端页面浏览数据
    * @param message
    * @param groupId
    * @return
    */
  def parseMBPageMessage(message:String, groupId: String):(String, String, Any) = {
    val pageTransformer = new PageinfoTransformer()
    val (startDateStr, endDateStr) = getDateFilter(groupId)
    println(s"=======>>正确的数据范围${startDateStr} ~ ${endDateStr}")
    pageTransformer.logParser(message, dimPage, dimEvent, fCate, startDateStr, endDateStr)
  }

  /**
    * 解析h5 页面浏览数据，包括pc weixin wap 以及 h5
    * @param message
    * @param groupId
    * @return
    */
  def parseH5Page(message:String, groupId: String):(String, String, Any) = {
    val h5LogTransformer = new H5PageTransformer()
    val (startDateStr, endDateStr) = getDateFilter(groupId)
    println(s"=======>>正确的数据范围${startDateStr} ~ ${endDateStr}")
    h5LogTransformer.logParser(message, dimPage, startDateStr, endDateStr)
  }

  /**
    * 解析h5 埋点点击数据，包括pc weixin wap 以及 h5
    * @param message
    * @param groupId
    * @return
    */
  def parseH5Event(message:String, groupId: String):(String, String, Any) = {
    val h5LogTransformer = new H5EventTransformer()
    val (startDateStr, endDateStr) = getDateFilter(groupId)
    println(s"=======>>正确的数据范围${startDateStr} ~ ${endDateStr}")
    h5LogTransformer.logParser(message, dimPage, dimH5EVENT, startDateStr, endDateStr)
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
    * Laa shay'a waqi'un mutlaq bale kouloun mumkin
    * 当其他人盲目的追寻真相(业务)和真实(绩效)的时候，记住
    **  万物皆虚。
    * 当其他人受到法律(需求)和道德(测试)的束缚的时候，记住。
    **  万事皆允。
    * 我们服侍光明(用户)却耕耘于黑暗(?)。
    **  我们是攻城狮。
    * @param args
    */
  def main(args: Array[String]) {

    println("======>> 2017-02-16 com.juanpi.bi.streaming.KafkaConsumer 开始运行，参数个数：" + args.length)

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
        println(arg)
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

    val dataBaseDir = ic.loadProperties._1

    println(s"数据落地目录：${dataBaseDir}")

    val ssc = ic.getStreamingContext()

    // 连接Kafka参数设置
    val kafkaParams : Map[String, String] = Map(
      "metadata.broker.list" -> brokerList,
      "auto.offset.reset" -> "largest",
      "group.id" -> groupId)

    // init beginning offset number, it could consumer which data with config file
    val km = new KafkaManager(kafkaParams, zkQuorum)

    // consumerType = "2", 用于当解析数据出错后，手动刷数据之用，需要手动指定offset
    if (consumerType.equals("2")) {
      println(s"=======>>consumerType=2,重复刷数据开始,consumerTime=${consumerTime}")
      km.setConfigOffset(Set(topic), groupId, consumerTime, ssc)
    }

    val message = km.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set(topic))

    // page 和 event 分开解析
    if(topic.equals("mb_pageinfo_hash2")) {
      val DimPage = InitConfig.initMBDim()._1
      val DimEvent = InitConfig.initMBDim()._2
      val DimFrontCate = InitConfig.initMBDim()._3
      val consumer = new KafkaConsumer(topic, dataBaseDir, DimPage, DimEvent, DimFrontCate, null, zkQuorum)
      consumer.pageProcess(groupId, message, ssc, km)
    } else if(topic.equals("mb_event_hash2")) {
      val DimPage = InitConfig.initMBDim()._1
      val DimEvent = InitConfig.initMBDim()._2
      val DimFrontCate = InitConfig.initMBDim()._3
      val consumer = new KafkaConsumer(topic, dataBaseDir, DimPage, DimEvent, DimFrontCate, null, zkQuorum)
      consumer.eventProcess(groupId, message, ssc, km)
    } else if(topic.equals("pc_events_hash3")) {
      val DimH5Page = InitConfig.initH5Dim()._1
      val DimH5Event = InitConfig.initH5Dim()._2
      val consumer = new KafkaConsumer(topic, dataBaseDir, DimH5Page, null, null, DimH5Event, zkQuorum)
      consumer.h5EventProcess(groupId, message, ssc, km)
    } else if(topic.equals("jp_hash3")) {
      val DimH5Page = InitConfig.initH5Dim()._1
      val consumer = new KafkaConsumer(topic, dataBaseDir, DimH5Page, null, null, null, zkQuorum)
      consumer.h5PageProcess(groupId, message, ssc, km)
    } else {
      println("请指定需要解析的kafka Topic-Group！")
      System.exit(1)
    }

    ssc.start()
    ssc.awaitTermination()
  }
}

