[TOC]

## 准备Jar
```
scp /data/jenkins_workspace/workspace/mb_pageinfo_real_direct_by_dw/realtime-sourcedata/target/realtime-souredata-1.0.jar hadoop@spark001.jp:/home/hadoop/users/gongzi/spark_reprod/
```

## spark-reprod-topic.sh
``` sh
#!/usr/bin/env bash

## shell name spark-reprod-topic.sh
## topic="mb_event_hash2"
## topic="mb_pageinfo_hash2"
## topic="pc_events_hash3"
## topic="jp_hash3"

### 对应的groupid：
## reprod_bi_realtime_by_dw_mb_event_hash2
## reprod_bi_realtime_by_dw_mb_pageinfo_hash2
## reprod_bi_realtime_by_dw_pc_events_hash3
## reprod_bi_realtime_by_dw_jp_hash3

if [ $# == 1 ]; then
    topic=$1
    groupId="reprod_bi_realtime_by_dw_${topic}"
    echo "=======>>group id is: $groupId"
    exit 22
else
    echo "args failed!"
    exit 2
fi

#params=("$@")
## 如果只修改了数据落地的目录，没有修改topic对应的groupId，会导致消费的Kafka的offset有冲突
## 因此topic不变，但是groupId需要变

jarPath="./realtime-souredata-1.0.jar"

echo "reprod com.juanpi.bi.streaming.KafkaConsumer $groupId start..."

/data/apache_projects/spark-hadoop-2.4.0/bin/spark-submit \
    --class com.juanpi.bi.streaming.KafkaConsumer \
    --master spark://GZ-JSQ-JP-BI-SPARK-MASTER-001.JP:7077,GZ-JSQ-JP-BI-SPARK-MASTER-002.JP:7077 \
    --deploy-mode client \
    --driver-memory 4g \
    --executor-memory 4g \
    --executor-cores 2 \
    --total-executor-cores 12 \
    --conf "spark.default.parallelism=24" \
    --driver-java-options "-XX:PermSize=1024M -XX:MaxPermSize=3072M -Xmx4096M -Xms2048M -Xmn1024M" \
    $jarPath "zkQuorum"="GZ-JSQ-JP-BI-KAFKA-001.jp:2181,GZ-JSQ-JP-BI-KAFKA-002.jp:2181,GZ-JSQ-JP-BI-KAFKA-003.jp:2181,GZ-JSQ-JP-BI-KAFKA-004.jp:2181,GZ-JSQ-JP-BI-KAFKA-005.jp:2181" "brokerList"="kafka-broker-000.jp:9082,kafka-broker-001.jp:9083,kafka-broker-002.jp:9084,kafka-broker-003.jp:9085,kafka-broker-004.jp:9086,kafka-broker-005.jp:9087,kafka-broker-006.jp:9092,kafka-broker-007.jp:9093,kafka-broker-008.jp:9094,kafka-broker-009.jp:9095,kafka-broker-010.jp:9096,kafka-broker-011.jp:9097" "topic"=$topic "groupId"=$groupId "consumerType"=1 "consumerTime"=60 "maxRecords"=80

if [ $? -ne 0 ];then
    echo "spark failed!"
    exit 2
fi

```

## reprod Topics
```
cd /home/hadoop/users/gongzi/spark_reprod

# 输出日志
nohup sh ./spark-reprod-topic.sh "mb_event_hash2" > ./mb_event_hash2.log 2>&1 &
nohup sh ./spark-reprod-topic.sh "mb_pageinfo_hash2" > ./mb_pageinfo_hash2.log 2>&1 &
nohup sh ./spark-reprod-topic.sh "pc_events_hash3" > ./pc_events_hash3.log 2>&1 &
nohup sh ./spark-reprod-topic.sh "jp_hash3" > ./jp_hash3.log 2>&1 &

# 输出日志
nohup sh ./spark-reprod-topic.sh "mb_event_hash2" > /dev/null 2>&1 &
nohup sh ./spark-reprod-topic.sh "mb_pageinfo_hash2" > /dev/null 2>&1 &
nohup sh ./spark-reprod-topic.sh "pc_events_hash3" > /dev/null 2>&1 &
nohup sh ./spark-reprod-topic.sh "jp_hash3" > /dev/null 2>&1 &
```

## 查看数据目录
```
# mb_event_hash2
hadoop fs -ls hdfs://nameservice1/user/hadoop/dw_realtime/reprod/mb_event_hash2/date=2017-02-25/gu_hash=4/logs

# mb_pageinfo_hash2
hadoop fs -ls hdfs://nameservice1/user/hadoop/dw_realtime/reprod/mb_pageinfo_hash2/date=2017-02-25/gu_hash=4/logs

# pc_events_hash3
hadoop fs -ls hdfs://nameservice1/user/hadoop/dw_realtime/reprod/pc_events_hash3/date=2017-02-25/gu_hash=4/logs

# jp_hash3
hadoop fs -ls hdfs://nameservice1/user/hadoop/dw_realtime/reprod/jp_hash3/date=2017-02-25/gu_hash=4/logs

```

