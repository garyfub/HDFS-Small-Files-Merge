package com.juanpi.bi.streaming

/**
 * Created by gongzi on 2016/7/9.
  * email:
 */
object Config {

  val interval = System.getProperty("spark.mystreaming.batch.interval", "60").toInt

  private val transformerPackage="com.juanpi.bi.transformer"
  val kafkaTopicMap:Map[String, Seq[String]] = Map(
                          "mb_event_hash2" -> List(s"${transformerPackage}.MbEventTransformer"),
                          "mb_pageinfo_hash2" -> List(s"${transformerPackage}.PageinfoTransformer"),
                          "pc_events_hash3" -> List(s"${transformerPackage}.H5EventTransformer"),
                          "jp_hash3" -> List(s"${transformerPackage}.H5PageTransformer")
                          )

  def getTopicTransformerClass(topic: String): String ={
    kafkaTopicMap.get(topic).get(0)
  }
}
