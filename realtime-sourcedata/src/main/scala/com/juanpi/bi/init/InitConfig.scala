package com.juanpi.bi.init

import java.util.Properties
import java.io.FileInputStream

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}

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
    // val zkQuorum="GZ-JSQ-JP-BI-HBASE-003.jp,GZ-JSQ-JP-BI-HBASE-001.jp,GZ-JSQ-JP-BI-HBASE-002.jp"
    hbaseConf.set("hbase.zookeeper.quorum", zkQuorum)
    hbaseConf.setInt("timeout", 120000)
    //Connection 的创建是个重量级的工作，线程安全，是操作hbase的入口
    ConnectionFactory.createConnection(hbaseConf)
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
