package com.juanpi.bi.streaming

import java.io.Serializable

import com.juanpi.bi.bean.{Event, Page, PageAndEvent, User}
import com.juanpi.bi.init.InitConfig
import com.juanpi.bi.transformer.ITransformer
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, _}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaManager

import scala.collection.mutable

// todo
import com.juanpi.bi.streaming.MultiOutputRDD._

@SerialVersionUID(42L)
class KafkaConsumer(topic: String, dimPage: mutable.HashMap[String, (Int, Int, String, Int)], dimEvent: mutable.HashMap[String, (Int, Int)], zkQuorum: String)
  extends Logging with Serializable {

  var transformer:ITransformer = null

  /**
    * event 过滤 collect_api_responsetime
    * page 和 event 都需要过滤 gu_id 为空的数据，需要过滤 site_id 不为（2, 3）的数据
    *
    * @param dataDStream
    * @param ssc
    * @param km
    */
  def process(dataDStream: DStream[(String,String)], ssc: StreamingContext, km: KafkaManager) = {
    // event 中直接顾虑掉 activityname = "collect_api_responsetime" 的数据
    // 需要查 utm 和 gu_id 的值，存在就取出来，否则写 hbase
    // 数据块中的每一条记录需要处理
    dataDStream.map(_._2.replace("\0",""))
      .filter(line => !line.contains("collect_api_responsetime"))
      .transform(transMessage _)
      .filter(!_._1.isEmpty)
      .foreachRDD((rdd, time) =>
      {
        val newRdd = rdd.map(record => {
//          println("------------->>::", record._1)
          val (user: User, pageAndEvent: PageAndEvent, page: Page, event: Event) = record._2
          val gu_id = user.gu_id
          val app_name = user.site_id match {
            case 1 => "jiu"
            case 2 => "zhe"
            case _ => ""
          }
          val (utm, gu_create_time) = HBaseHandler.getGuIdUtmInitDate(zkQuorum, gu_id + app_name)
          user.utm = utm
          user.gu_create_time = gu_create_time
          (record._1, List(user, pageAndEvent, page, event).mkString("\u0001"))
        })
        // 保存数据至hdfs
        newRdd.map(v => (v._1+"/"+time.milliseconds,v._2))
          .saveAsMultiTextFiles(Config.baseDir+"/"+topic)
      })

    // 更新kafka offset
    dataDStream.foreachRDD { rdd =>
      km.updateOffsets(rdd)
    }
  }

  def parseMessage(message:String):(String, Any) = {
    getTransformer().transform(message, dimPage, dimEvent)
  }

  def transMessage(rdd:RDD[String]):RDD[(String, Any)] = {
    rdd.map { msg => parseMessage(msg) }
  }

  def getTransformer():ITransformer = {
    if(transformer == null){
      transformer = Class.forName(Config.getTopicTransformerClass(topic)).newInstance().asInstanceOf[ITransformer]
    }
    transformer
  }

  // 保存 page 或者 event的数据
  def save(page_event: DStream[(String, String)]) = {
    page_event.foreachRDD{ (rdd,time) =>
      rdd.map(v => (v._1+"/"+time.milliseconds,v._2))
        .repartition(1)
        .saveAsMultiTextFiles(Config.baseDir+"/"+topic)
    }
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

  /**
    * "zkQuorum":"GZ-JSQ-JP-BI-KAFKA-001.jp:2181,GZ-JSQ-JP-BI-KAFKA-002.jp:2181,GZ-JSQ-JP-BI-KAFKA-003.jp:2181,GZ-JSQ-JP-BI-KAFKA-004.jp:2181,GZ-JSQ-JP-BI-KAFKA-005.jp:2181" "brokerList":"kafka-broker-000.jp:9082,kafka-broker-001.jp:9083,kafka-broker-002.jp:9084,kafka-broker-003.jp:9085,kafka-broker-004.jp:9086,kafka-broker-005.jp:9087,kafka-broker-006.jp:9092,kafka-broker-007.jp:9093,kafka-broker-008.jp:9094,kafka-broker-009.jp:9095,kafka-broker-010.jp:9096,kafka-broker-011.jp:9097" "topic":"mb_pageinfo_hash2" "groupId":"pageinfo_direct_dw" "consumerType":1 "consumerTime":5
    *
    * @param args
    */
  def main(args: Array[String]) {

    println("======>> com.juanpi.bi.streaming.KafkaConsumer 开始运行，参数个数：" + args.length)

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
        """.stripMargin)
      System.exit(1)
    }

    var (zkQuorum, brokerList, topic, groupId, consumerType, consumerTime) = ("", "", "", "", "1", "60")

    println("com.juanpi.bi.streaming.KafkaConsumer 开始运行，传入参数：")
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
        }
      }
    )

    val groupIds = Set("bi_mb_pageinfo_real_direct_by_dw", "bi_mb_event_real_direct_by_dw")
    if(!groupIds.contains(groupId)) {
      println("groupId有误！！约定的groupId是：mbevent_direct_dw 或者 pageinfo_direct_dw")
      System.exit(1)
    }

    if(!Config.kafkaTopicMap.contains(topic)){
      System.err.println(s"没有找到表:${topic}配置信息")
      System.exit(1)
    }

    // 初始化 SparkConfig StreamingContext HiveContext
    val ic = InitConfig
    ic.initParam(groupId, Config.interval)
    val ssc = ic.getStreamingContext()

    // 连接Kafka参数设置
    val kafkaParams : Map[String, String] = Map(
      "metadata.broker.list" -> brokerList,
      "auto.offset.reset" -> "largest",
      "group.id" -> groupId)

    // init beginning offset number, it could consumer which data with config file
    val km = new KafkaManager(kafkaParams, zkQuorum)

    // consumerType = "2", 用于当解析数据出错后，手动刷数据之用
    // 需要手动指定offset
    // 运行之前需要跟架构沟通
    if (consumerType.equals("2")) {
      km.setConfigOffset(Set(topic), groupId, consumerTime, ssc)
    }

    val message = km.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set(topic))
    val consumer = new KafkaConsumer(topic, ic.DIMPAGE, ic.DIMENT, zkQuorum)
    consumer.process(message, ssc, km)

    ssc.start()
    ssc.awaitTermination()
  }
}

