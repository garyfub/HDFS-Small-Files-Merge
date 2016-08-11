package org.apache.spark.streaming.kafka

import java.text.SimpleDateFormat

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder
import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaCluster._
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper.{CreateMode, WatchedEvent, Watcher, ZooKeeper}

import scala.reflect.ClassTag

class KafkaManager(val kafkaParams: Map[String, String],
                   val zkQum: String) extends Watcher {

  private val kc = new KafkaCluster(kafkaParams)
  // private val zc = new ZooKeeperClient(Amount.of(0, Time.MILLISECONDS), new InetSocketAddress(zkServer,zkPort))
  private var lastRunTime = System.currentTimeMillis()
  private var useTopics = Set[String]()

  override def process(event: WatchedEvent): Unit = {
    println("event in watcher:" + event.getPath)
  }

  def setConfigOffset(topics: Set[String], groupId: String, selectTime: String, ssc: StreamingContext): Unit = {
    //don't support multiple topics,maybe fix it.
    val topic = topics.head
    val configReadPath = "/" + topic + "_" + groupId + "/" + selectTime
    //val configOffset = zkUtils.get(zc, configReadPath)
    val zc = new ZooKeeper(zkQum, 30000, this)
    if (zc.exists(configReadPath, false) == null) {
      println("ERROR,can't find " + configReadPath + "in zookeeper!")
      zc.close()
      ssc.stop()
      System.exit(1)
    }
    val configOffset = zc.getData(configReadPath, false, null)

    if (configOffset.isEmpty) {
      println("ERROR, can't find the config timestamp's kafka offset data!")
      zc.close()
      ssc.stop()
      System.exit(1)
    }

    zc.close()
    val offsets = new String(configOffset).split("#").map(line => {
      val mapTuple = line.split("-")
      val tupleFields = mapTuple(0).split(",")
      val result = mapTuple(1).toLong
      val topicInfo = TopicAndPartition(tupleFields(0), tupleFields(1).toInt)
      (topicInfo, result)
    }).toMap[TopicAndPartition, Long]

    val o = kc.setConsumerOffsets(groupId, offsets)
    if (o.isLeft) {
      println(s"Error updating the offset to Kafka cluster: ${o.left.get}")
    }
  }

  def createTopicDirectStream[K: ClassTag, V: ClassTag,
  KD <: Decoder[K] : ClassTag,
  VD <: Decoder[V] : ClassTag](ssc: StreamingContext,
                               kafkaParams: Map[String, String],
                               topics: Set[String]): InputDStream[(String, V)] = {
    val groupId = kafkaParams.get("group.id").get
    /*if consumer type is 2, it could consumer everytime's offset in kafka*/
    /*consumer type is 1, means it consumer the lastest/beginning offset in kafka with config*/


    useTopics = topics
    /*check offset status to use it.*/
    setOrUpdateOffsets(topics, groupId)

    val partitionsE = kc.getPartitions(topics)
    if (partitionsE.isLeft)
      throw new SparkException(s"get kafka partition failed: ${partitionsE.left.get}")
    val partitions = partitionsE.right.get

    val consumerOffsetsE = kc.getConsumerOffsets(groupId, partitions)
    if (consumerOffsetsE.isLeft)
      throw new SparkException(s"get kafka consumer offsets failed: ${consumerOffsetsE.left.get}")
    val consumerOffsets = consumerOffsetsE.right.get

    val messages = KafkaUtils.createDirectStream[K, V, KD, VD, (String, V)](
      ssc, kafkaParams, consumerOffsets, (mmd: MessageAndMetadata[K, V]) => (mmd.topic, mmd.message))
    messages
  }


  def createDirectStream[K: ClassTag, V: ClassTag,
  KD <: Decoder[K] : ClassTag,
  VD <: Decoder[V] : ClassTag](ssc: StreamingContext,
                               kafkaParams: Map[String, String],
                               topics: Set[String]): InputDStream[(K, V)] = {
    val groupId = kafkaParams.get("group.id").get
    /*if consumer type is 2, it could consumer everytime's offset in kafka*/
    /*consumer type is 1, means it consumer the lastest/beginning offset in kafka with config*/


    useTopics = topics
    /*check offset status to use it.*/
    setOrUpdateOffsets(topics, groupId)

    val partitionsE = kc.getPartitions(topics)
    if (partitionsE.isLeft)
      throw new SparkException(s"get kafka partition failed: ${partitionsE.left.get}")
    val partitions = partitionsE.right.get

    val consumerOffsetsE = kc.getConsumerOffsets(groupId, partitions)
    if (consumerOffsetsE.isLeft)
      throw new SparkException(s"get kafka consumer offsets failed: ${consumerOffsetsE.left.get}")
    val consumerOffsets = consumerOffsetsE.right.get

    val messages = KafkaUtils.createDirectStream[K, V, KD, VD, (K, V)](
      ssc, kafkaParams, consumerOffsets, (mmd: MessageAndMetadata[K, V]) => (mmd.key, mmd.message))
    messages
  }

  private def getConfigOffsets(partitions: Set[TopicAndPartition]): Map[TopicAndPartition, Long] = {
    val reset = kafkaParams.get("auto.offset.reset").map(_.toLowerCase)
    var leaderOffsets: Map[TopicAndPartition, LeaderOffset] = null

    if (reset == Some("smallest")) {
      val leaderOffsetsE = kc.getEarliestLeaderOffsets(partitions)
      if (leaderOffsetsE.isLeft)
        throw new SparkException(s"get earliest leader offsets failed: ${leaderOffsetsE.left.get}")
      leaderOffsets = leaderOffsetsE.right.get
    } else {
      val leaderOffsetsE = kc.getLatestLeaderOffsets(partitions)
      if (leaderOffsetsE.isLeft)
        throw new SparkException(s"get latest leader offsets failed: ${leaderOffsetsE.left.get}")
      leaderOffsets = leaderOffsetsE.right.get
    }

    val offsets = leaderOffsets.map {
      case (tp, offset) => (tp, offset.offset)
    }

    offsets
  }

  private def setOrUpdateOffsets(topics: Set[String], groupId: String): Unit = {
    topics.foreach(topic => {
      var hasConsumed = true
      val partitionsE = kc.getPartitions(Set(topic))
      if (partitionsE.isLeft) {
        hasConsumed = false
      }
      val partitions = partitionsE.right.get
      if (partitionsE.isLeft)
        throw new SparkException(s"get kafka partition failed: ${partitionsE.left.get}")

      val consumerOffsetsE = kc.getConsumerOffsets(groupId, partitions)
      if (consumerOffsetsE.isLeft) hasConsumed = false
      ///throw new SparkException(s"get kafka partition failed: ${partitionsE.left.get}")

      if (hasConsumed) {
        // 消费过
        /**
          * 如果streaming程序执行的时候出现kafka.common.OffsetOutOfRangeException，
          * 说明zk上保存的offsets已经过时了，即kafka的定时清理策略已经将包含该offsets的文件删除。
          * 针对这种情况，只要判断一下zk上的consumerOffsets和earliestLeaderOffsets的大小，
          * 如果 consumerOffsets 比 earliestLeaderOffsets 还小的话，说明 consumerOffsets 已过时,
          * 这时把 consumerOffsets 更新为 earliestLeaderOffsets
          */
        val earliestLeaderOffsetsE = kc.getEarliestLeaderOffsets(partitions)
        if (earliestLeaderOffsetsE.isLeft)
          throw new SparkException(s"get earliest leader offsets failed: ${earliestLeaderOffsetsE.left.get}")
        val earliestLeaderOffsets = earliestLeaderOffsetsE.right.get
        val consumerOffsets = consumerOffsetsE.right.get

        // 可能只是存在部分分区consumerOffsets过时，所以只更新过时分区的consumerOffsets为earliestLeaderOffsets
        var offsets: Map[TopicAndPartition, Long] = Map()
        consumerOffsets.foreach({ case (tp, n) =>
          val earliestLeaderOffset = earliestLeaderOffsets(tp).offset
          if (n < earliestLeaderOffset) {
            println("consumer group:" + groupId + ",topic:" + tp.topic + ",partition:" + tp.partition +
              " offsets已经过时，更新为" + earliestLeaderOffset)
            offsets += (tp -> earliestLeaderOffset)
          }
        })

        if (!offsets.isEmpty) {
          kc.setConsumerOffsets(groupId, offsets)
        }
      } else {
        // 没有消费过
        val offsets = getConfigOffsets(partitions)
        kc.setConsumerOffsets(groupId, offsets)
      }
    })
  }

  def updateOffsets(rdd: RDD[(String, String)]): Unit = {
    val groupId = kafkaParams.get("group.id").get
    val offsetsList = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

    for (offsets <- offsetsList) {
      val topicAndPartition = TopicAndPartition(offsets.topic, offsets.partition)
      val o = kc.setConsumerOffsets(groupId, Map((topicAndPartition, offsets.untilOffset)))
      if (o.isLeft) {
        println(s"Error updating the offset to Kafka cluster: ${o.left.get}")
      }
    }

    val offsetMap = offsetsList.map(offset => {
      (offset.topic + "," + offset.partition + "-" + offset.untilOffset)
    }).mkString("#")

    val currentTime = System.currentTimeMillis()
    if (currentTime - lastRunTime >= 1000 * 2) {
      val sdf = new SimpleDateFormat("yyyyMMddHHmm")
      val dayDate: String = try {
        sdf.format(currentTime / 1000 / 600 * 600 * 1000)
      } catch {
        case _: Throwable => {
          "197201010110"
        }
      }
      val topic = useTopics.head
      /// zkUtils.setOrCreate(zc, createPath, offsetMap)
      val nodePath = "/" + topic + "_" + groupId
      val createPath = nodePath + "/" + dayDate

      val zc = new ZooKeeper(zkQum, 30000, this)
      if (zc.exists(nodePath, false) == null) {
        val ret = zc.create(nodePath, "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
        println("create path" + nodePath)
      }

      if (zc.exists(createPath, false) == null) {
        zc.create(createPath, offsetMap.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
      } else {
        zc.setData(createPath, offsetMap.getBytes(), -1)
      }
      lastRunTime = currentTime
      zc.close()
    }

  }
}
