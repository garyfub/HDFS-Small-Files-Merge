package com.juanpi.bi.streaming

import com.typesafe.config.ConfigFactory


/**
 * Created by gongzi on 2016/7/9.
  * email:
 */
object Config {

  val interval = System.getProperty("spark.mystreaming.batch.interval", "60").toInt

  val dirPrefix = "/user/hadoop/dw_realtime/"

  // todo 配置 baseDir
  val baseDir = System.getProperty("spark.juanpi.bi.realtime.basedir", dirPrefix + "dw_real_for_path_list")

  private val transformerPackage="com.juanpi.bi.transformer"
  val kafkaTopicMap:Map[String, Seq[String]] = Map(
                          "mb_event_hash2" -> List(s"${transformerPackage}.MbEventTransformer"),
                          "mb_pageinfo_hash2" -> List(s"${transformerPackage}.PageinfoTransformer"),
                          "pc_events_hash3" -> List(s"${transformerPackage}.H5EventTransformer"),
                          "jp_hash3" -> List(s"${transformerPackage}.H5PageTransformer")
                          )

  def loadProperties():Unit = {

    val con = ConfigFactory.load("config.properties");
    val dataBaseDir = con.getString("dataBaseDir");
    val kafkaTopicIds = con.getString("kafkaTopicIds");

//    val (dataBaseDir, kafkaTopicIds) =
//      try {
//        val prop = new Properties()
//        getClass.getResource("config.properties")
//        prop.load(new FileInputStream("config.properties"))
//
//        (
//          prop.getProperty("dataBaseDir"),
//          prop.getProperty("KafkaTopicIds")
//        )
//      } catch { case e: Exception =>
//        e.printStackTrace()
//        sys.exit(1)
//      }
    println(dataBaseDir, kafkaTopicIds)
  }

  def getTopicTransformerClass(topic: String): String ={
    kafkaTopicMap.get(topic).get(0)
  }

  def main(args: Array[String]) {

//    println(getTopicTransformerClass("mbevent"))
//    println(getTopicTransformerClass("pageinfo"))
//    println(interval)
    loadProperties()
  }
}
