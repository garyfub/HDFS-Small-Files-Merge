package com.juanpi.bi.streaming

import com.juanpi.bi.init.InitConfig
import com.juanpi.bi.transformer.ITransformer
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaManager
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{Logging, SparkConf, SparkContext}

// todo
import com.juanpi.bi.streaming.MultiOutputRDD._

@SerialVersionUID(42L)
class KafkaConsumer(topic: String, dimPage: RDD[(Int, (String, String))], dimEvent: RDD[(Int, (String, String))])
  extends Logging with Serializable
{

  var transformer:ITransformer = null

  def process(dataDStream: DStream[(String,String)], ssc: StreamingContext, km: KafkaManager) ={
    val data = dataDStream.map(_._2.replace("\0","")).transform(transMessage _)
    save(data)

    // save offset
    dataDStream.foreachRDD { rdd =>
      km.updateOffsets(rdd)
    }
  }

  def transMessage(rdd:RDD[String]):RDD[(String,String)] ={
    rdd.map{msg =>
      parseMessage(msg)
    }
  }

  def parseMessage(message:String):(String,String) = {
    getTransformer().transform(message, dimPage, dimEvent)
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

  def initDimPage(sqlContext: HiveContext): RDD[(Int, (String, String))] =
  {
    val dimPageSql = s"""select * from dw.dim_page'""".stripMargin

    val dimPageData = sqlContext.sql(dimPageSql).persist(StorageLevel.MEMORY_AND_DISK)

    val dimPage = dimPageData.map(line => {
      val page_id = line.getAs[Int]("page_id")
      val page_exp1 = line.getAs[String]("page_exp1")
      val page_exp2 = line.getAs[String]("page_exp2")
      (page_id, (page_exp1, page_exp2))
    })

    dimPageData.unpersist(true)

    dimPage
  }

  def initDimEvent(sqlContext: HiveContext): RDD[(Int, (String, String))] =
  {
    val dimEventSql = s"""select * from dw.dim_event'""".stripMargin

    val dimEventData = sqlContext.sql(dimEventSql).persist(StorageLevel.MEMORY_AND_DISK)

    val dimEvent = dimEventData.map(line => {
      val event_id = line.getAs[Int]("page_id")
      val event_exp1 = line.getAs[String]("event_exp1")
      val event_exp2 = line.getAs[String]("event_exp2")

      (event_id, (event_exp1, event_exp2))
    })

    dimEventData.unpersist(true)

    dimEvent
  }

  def main(args: Array[String]) {
    if (args.length != 2) {
      System.err.println(s"""
                            |Usage: KafkaConsumer <zkQuorum> <brokers> <topic> <groupId> <consumerType> <consumerTime>
                            |OR
                            |Usage: KafkaConsumer <topic> <groupId>
                            |  #<zkQuorum> zookeeper address to save kafka consumer offsets
                            |  #<brokers> is a list of one or more Kafka brokers
                            |  <topic> topic name
                            |  <groupId> consumer groupId name
                            |  #<consumerType> consumer type 1-every batch offset save 2-recove from 5 minutes save datetime
                            |  #<consumerTime> recoved time
        """.stripMargin)
      System.exit(1)
    }
    val ic = new InitConfig()
    println(ic.brokerList, ic.consumerTime, ic.hbaseZk, ic.zkQuorum)

    // read from system env
    val Array(topic, groupId) = args
    println(topic)

    // read from conf.properties
    val Array(brokerList, zkQuorum, consumerType, consumerTime, hbaseZk) = Array(ic.brokerList, ic.zkQuorum, ic.consumerType, ic.consumerTime, ic.hbaseZk)
    val groupIds = Set("pageinfo_direct_dw", "mbevent_direct_dw")
    if(!groupIds.contains(groupId)) {
      println("groupId有误！！约定的groupId是：mbevent_direct_dw 或者 pageinfo_direct_dw")
      System.exit(1)
    }

    if(!Config.kafkaTopicMap.contains(topic)){
      System.err.println(s"没有找到表:${topic}配置信息")
      System.exit(1)
    }

    val conf = new SparkConf()
      .setAppName("com.juanpi.bi.realtime." + topic + ".Consumer")
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

    val ssc = new StreamingContext(conf, Seconds(Config.interval))
    val sc: SparkContext = new SparkContext(conf)

    // 查询 hive 中的 dim_page 和 dim_event
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    val dimPage = initDimPage(sqlContext)
    val dimEvent = initDimEvent(sqlContext)

    // Connect to a Kafka topic for reading
    val kafkaParams : Map[String, String] = Map("metadata.broker.list" -> brokerList,
      "auto.offset.reset" -> "largest",
      "group.id" -> groupId)

    /*init beginning offset number, it could consumer which data with config file*/
    val km = new KafkaManager(kafkaParams, zkQuorum)

    if (consumerType.equals("2")) {
      km.setConfigOffset(Set(topic), groupId, consumerTime, ssc)
    }

    val message = km.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set(topic))
    val consumer = new KafkaConsumer(topic, dimPage, dimEvent)
    consumer.process(message, ssc, km)

    ssc.start()
    ssc.awaitTermination()
  }
}

