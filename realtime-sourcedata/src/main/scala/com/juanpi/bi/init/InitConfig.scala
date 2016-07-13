package com.juanpi.bi.init

import java.util.Properties
import java.io.FileInputStream

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel
import play.api.libs.json.{JsValue, Json}

/**
  * Created by gongzi on 2016/7/8.
  */
class InitConfig {

  private[this] var _brokerList: String = ""

  def brokerList: String = _brokerList

  def brokerList_=(value: String): Unit = {
    _brokerList = value
  }

  var groupId = ""
  var zkQuorum = ""
  var consumerType = ""
  var consumerTime = ""
  var hbaseZk = ""
  var dimPages: RDD[(String, Int, String)]
  var dimEvents: RDD[(String, Int, String)]

  this.loadProperties()

  def loadProperties():Unit = {
    val properties = new Properties()
    val path = Thread.currentThread().getContextClassLoader.getResource("conf.properties").getPath //文件要放到resource文件夹下
    properties.load(new FileInputStream(path))
    // 读取键为ddd的数据的值
    brokerList_=(properties.getProperty("brokerList"))
    groupId = properties.getProperty("groupId")
    zkQuorum = properties.getProperty("zkQuorum")
    consumerType = properties.getProperty("consumerType")
    consumerTime = properties.getProperty("consumerTime")
//    topic = properties.getProperty("topic")
    hbaseZk = properties.getProperty("hbaseZk")
//    println(properties.getProperty("ddd","没有值"))//如果ddd不存在,则返回第二个参数
  }

  def getHbaseConf(): Connection = {
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.set("hbase.zookeeper.quorum", zkQuorum)
    hbaseConf.setInt("timeout", 120000)
    //Connection 的创建是个重量级的工作，线程安全，是操作hbase的入口
    ConnectionFactory.createConnection(hbaseConf)
  }

  def iniHive() = {
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
  }

  def initDimPage(sqlContext: HiveContext): RDD[(String, Int, String)] =
  {
    val dimPageSql = s"""select page_id,page_exp1, page_exp2, concat_ws(",", url1, url2, url3,regexp1, regexp2, regexp3) as url_pattern
                         | from dim_page where page_id>0 order by page_id'""".stripMargin

    val dimPageData = sqlContext.sql(dimPageSql).persist(StorageLevel.MEMORY_AND_DISK)

    dimPages = dimPageData.map(line => {
      val page_id = line.getAs[Int]("page_id")
      val page_exp1 = line.getAs[String]("page_exp1")
      val page_exp2 = line.getAs[String]("page_exp2")
      val url_pattern = line.getAs[String]("url_pattern")
      (page_exp1+page_exp2, page_id, url_pattern)
    })

    dimPageData.unpersist(true)

    dimPages
  }
}

object InitConfig {
  def main(args: Array[String]) {
    val ic = new InitConfig()
    ic.loadProperties
    println(ic.brokerList)

    println(ic.brokerList, ic.consumerTime, ic.groupId, ic.hbaseZk, ic.zkQuorum)
  }
}
