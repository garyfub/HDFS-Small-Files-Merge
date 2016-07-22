package com.juanpi.bi.streaming

import com.juanpi.bi.init.InitConfig
import com.juanpi.bi.transformer.ITransformer
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaManager
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.Logging

import scala.collection.mutable

// todo
import com.juanpi.bi.streaming.MultiOutputRDD._

@SerialVersionUID(42L)
class KafkaConsumer(topic: String, dimpage: mutable.HashMap[String, (Int, Int, String, Int)])
  extends Logging with Serializable
{

  var transformer:ITransformer = null

  /**
    * event 过滤 collect_api_responsetime
    * page 和 event 都需要过滤 gu_id 为空的数据，需要过滤 site_id 不为（2, 3）的数据
    * @param dataDStream
    * @param ssc
    * @param km
    */
  def process(dataDStream: DStream[(String,String)], ssc: StreamingContext, km: KafkaManager) ={
    // event 中直接顾虑掉 activityname = "collect_api_responsetime" 的数据
    // 需要查 utm 和 gu_id 的值，存在就取出来，否则写 hbase
    val data = dataDStream.map(_._2.replace("\0","")).filter(line => !line.contains("collect_api_responsetime")).transform(transMessage _)
    save(data)

    // save offset
    dataDStream.foreachRDD { rdd =>
      km.updateOffsets(rdd)
    }
  }

  def transMessage(rdd:RDD[String]):RDD[(String,String)] ={
    rdd.map{ msg =>parseMessage(msg) }
  }

  def parseMessage(message:String):(String,String) = {
    getTransformer().transform(message, dimpage)
  }

  def getTransformer():ITransformer = {
    if(transformer == null){
      transformer = Class.forName(Config.getTopicTransformerClass(topic)).newInstance().asInstanceOf[ITransformer]
    }
    transformer
  }

  // 保存 page 或者 event的数据
  def save(page_event:DStream[(String,String)]) = {
    page_event.foreachRDD{ (rdd,time) =>
      rdd.map(v => (v._1+"/"+time.milliseconds,v._2))
        .repartition(1)
        .saveAsMultiTextFiles(Config.baseDir+"/"+topic)
    }
  }
}


object KafkaConsumer{

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

    val Array(zkQuorum, brokerList, topic, groupId, consumerType, consumerTime) = args

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
    val consumer = new KafkaConsumer(topic, ic.DIMPAGE)
    consumer.process(message, ssc, km)

    ssc.start()
    ssc.awaitTermination()
  }
}

