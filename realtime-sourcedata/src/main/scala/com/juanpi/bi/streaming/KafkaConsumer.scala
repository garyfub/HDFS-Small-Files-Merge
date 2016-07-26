package com.juanpi.bi.streaming

import java.util

import com.juanpi.bi.bean.{Event, Page, PageAndEvent, User}
import com.juanpi.bi.init.InitConfig
import com.juanpi.bi.transformer.ITransformer
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaManager
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.Logging
import org.apache.hadoop.hbase.client.{ConnectionFactory, _}
import org.apache.hadoop.hbase.util.Bytes
import play.libs.Json

import scala.collection.mutable

// todo
import com.juanpi.bi.streaming.MultiOutputRDD._

@SerialVersionUID(42L)
class KafkaConsumer(topic: String, dimpage: mutable.HashMap[String, (Int, Int, String, Int)], hbaseTable: Table)
  extends Logging with Serializable {

  var transformer:ITransformer = null
  val HbaseFamily = "dw"

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
        .foreachRDD((rdd,time) =>
        {
          val newRdd = rdd.map(record => {
            val (user: User, pageAndEvent: PageAndEvent, page: Page, event: Event) = record._2
            val gu_id = user.gu_id
            val (utm, gu_create_time) = getGuIdUtmInitDate(gu_id)
            user.utm_id = utm
            user.gu_create_time = gu_create_time
            (record._1, List(user, pageAndEvent, page, event).mkString("\u0001"))
          })
          // 保存数据至hdfs

          newRdd.map(v => (v._1+"/"+time.milliseconds,v._2))
            .repartition(1)
            .saveAsMultiTextFiles(Config.baseDir+"/"+topic)
        })

    // 更新kafka offset
    dataDStream.foreachRDD { rdd =>
      km.updateOffsets(rdd)
    }
  }


  /**
    *
    * @return
    */
  private def getHbaseConf(): Connection = {
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "")
    hbaseConf.setInt("timeout", 120000)
    //Connection 的创建是个重量级的工作，线程安全，是操作hbase的入口
    ConnectionFactory.createConnection(hbaseConf)
  }


  /**
    * 查hbase 从 ticks_history 中查找 ticks 存在的记录
    *
    * @param gu_id
    * @return
    */
  private def getGuIdUtmInitDate(gu_id: String) = {
    var utm = ""
    var gu_create_time = ""
    val ticks_history = hbaseTable
    val key = new Get(Bytes.toBytes(gu_id))
    println("=======> ticks_history.get:" + key)
    val ticks_res = ticks_history.get(key)

    if (!ticks_res.isEmpty) {
      utm = Bytes.toString(ticks_res.getValue(HbaseFamily.getBytes, "utm".getBytes))
      gu_create_time = Bytes.toString(ticks_res.getValue(HbaseFamily.getBytes, "init_date".getBytes))
      (utm, gu_create_time)
    }
    else {
      // 如果不存在就写入 hbase
      // 准备插入一条 key 为 id001 的数据
      val p = new Put(gu_id.getBytes)
      // 为put操作指定 column 和 value （以前的 put.add 方法被弃用了）
      p.addColumn(HbaseFamily.getBytes, "utm".getBytes, utm.getBytes)
      p.addColumn(HbaseFamily.getBytes, "init_date".getBytes, gu_create_time.getBytes)
      //提交
      ticks_history.put(p)
      (utm, gu_create_time)
    }
  }

  def transMessage(rdd:RDD[String]):RDD[(String, Any)] = {
    rdd.map{ msg =>parseMessage(msg) }
  }

  def parseMessage(message:String):(String, Any) = {
    getTransformer().transform(message, dimpage)
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

case class SparkKafkaParam (

                           )

object KafkaConsumer{

  def main(args: Array[String]) {

    println("======>> com.juanpi.bi.streaming.KafkaConsumer 开始运行，参数个数：" + args.length)

    val arg = args(0)
    println(arg)

    val argJson = Json.parse(arg)
    println("======>> com.juanpi.bi.streaming.KafkaConsumer 开始运行，参数个数：" + Array(argJson.fields()).length)

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

    // test json 格式的参数，结果结果接受参数的时候，直接将double quote 去掉了，如果用转移符，反而又比较麻烦，因此放弃掉
//    val str =
//      """
//        |{"zkQuorum":"GZ-JSQ-JP-BI-KAFKA-001.jp:2181,GZ-JSQ-JP-BI-KAFKA-002.jp:2181,GZ-JSQ-JP-BI-KAFKA-003.jp:2181,GZ-JSQ-JP-BI-KAFKA-004.jp:2181,GZ-JSQ-JP-BI-KAFKA-005.jp:2181",
//        |"brokerList":"kafka-broker-000.jp:9082,kafka-broker-001.jp:9083,kafka-broker-002.jp:9084,kafka-broker-003.jp:9085,kafka-broker-004.jp:9086,kafka-broker-005.jp:9087,kafka-broker-006.jp:9092,kafka-broker-007.jp:9093,kafka-broker-008.jp:9094,kafka-broker-009.jp:9095,kafka-broker-010.jp:9096,kafka-broker-011.jp:9097",
//        |"topic":"mb_pageinfo_hash2",
//        |"groupId":"pageinfo_direct_dw",
//        |"consumerType":1,
//        |"consumerTime":5}"
//      """.stripMargin

    var (zkQuorum, brokerList, topic, groupId, consumerType, consumerTime) = ("", "", "", "", "1", "60")

    val it: util.Iterator[String] = argJson.fieldNames()
    while(it.hasNext) {
      val name = it.next()
      name match {
        case "zkQuorum" => zkQuorum = argJson.get("zkQuorum").toString
        case "brokerList" => brokerList = argJson.get("brokerList").toString
        case "topic" => topic = argJson.get("topic").toString
        case "groupId" => groupId = argJson.get("topic").toString
        case "consumerType" => consumerType = argJson.get("consumerType").toString
        case "consumerTime" => consumerTime = argJson.get("consumerTime").toString
      }
    }

    println(zkQuorum, brokerList, topic, groupId, consumerType, consumerTime)
    System.exit(1)
//    val Array(zkQuorum, brokerList, topic, groupId, consumerType, consumerTime) = args

    val groupIds = Set("pageinfo_direct_dw", "mbevent_direct_dw")
    if(!groupIds.contains(groupId)) {
      println("groupId有误！！约定的groupId是：mbevent_direct_dw 或者 pageinfo_direct_dw")
      System.exit(1)
    }

    if(!Config.kafkaTopicMap.contains(topic)){
      System.err.println(s"没有找到表:${topic}配置信息")
      System.exit(1)
    }

    /**
      * 初始化 SparkConfig StreamingContext HiveContext
      *
      */
    val ic = InitConfig
    ic.initParam(topic, Config.interval)
    val ssc = ic.getStreamingContext()

    // Connect to a Kafka topic for reading
    val kafkaParams : Map[String, String] = Map(
      "metadata.broker.list" -> brokerList,
      "auto.offset.reset" -> "largest",
      "group.id" -> groupId)

    // init beginning offset number, it could consumer which data with config file
    val km = new KafkaManager(kafkaParams, zkQuorum)

    if (consumerType.equals("2")) {
      km.setConfigOffset(Set(topic), groupId, consumerTime, ssc)
    }

    val message = km.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set(topic))
    val consumer = new KafkaConsumer(topic, ic.DIMPAGE, ic.initTicksHistory())
    consumer.process(message, ssc, km)

    ssc.start()
    ssc.awaitTermination()
  }
}

