
##### jar包上传
```
### on ops001.jp
### 删除 ops001.jp 和 sparkoo1.jp 上的 realtime-souredata-1.0-SNAPSHOT-jar-with-dependencies.jar 后
scp /data/home/gongzi/realtime-souredata-1.0-SNAPSHOT-jar-with-dependencies.jar hadoop@spark001.jp:/home/hadoop/users/gongzi/

scp ~/dev_pro/dw-realtime/realtime-sourcedata/target/realtime-souredata-1.0-SNAPSHOT-jar-with-dependencies.jar hadoop@spark001.jp:/home/hadoop/users/gongzi/

ssh hadoop@spark001.jp
# on sparkoo1.jp

hadoop fs -mkdir /user/hadoop/spark-jobs/gongzi

hadoop fs -du -h hdfs://nameservice1/user/hadoop/gongzi/dw_real_for_path_list/mb_event_hash2/date=2016-08-28
hadoop fs -du -h hdfs://nameservice1/user/hadoop/gongzi/dw_real_for_path_list/mb_pageinfo_hash2/date=2016-08-21

#### 架构做的实时解析的数据参考
[hadoop@GZ-JSQ-JP-BI-SPARK-001 gongzi]$   hadoop fs -du -h hdfs://nameservice1/user/hadoop/kafka_realoutput/mbevent
0        hdfs://nameservice1/user/hadoop/kafka_realoutput/mbevent/_SUCCESS
5.1 G    hdfs://nameservice1/user/hadoop/kafka_realoutput/mbevent/date=2016-04-10
51.5 K   hdfs://nameservice1/user/hadoop/kafka_realoutput/mbevent/date=2016-04-28
153.1 K  hdfs://nameservice1/user/hadoop/kafka_realoutput/mbevent/date=2016-04-29
24.1 G   hdfs://nameservice1/user/hadoop/kafka_realoutput/mbevent/date=2016-08-27
21.8 G   hdfs://nameservice1/user/hadoop/kafka_realoutput/mbevent/date=2016-08-28
4.2 G    hdfs://nameservice1/user/hadoop/kafka_realoutput/mbevent/date=2016-08-29

[hadoop@GZ-JSQ-JP-BI-SPARK-001 gongzi]$   hadoop fs -du -h hdfs://nameservice1/user/hadoop/kafka_realoutput/pageinfo
4.7 G    hdfs://nameservice1/user/hadoop/kafka_realoutput/pageinfo/date=2016-04-10
154.2 K  hdfs://nameservice1/user/hadoop/kafka_realoutput/pageinfo/date=2016-04-28
291.6 K  hdfs://nameservice1/user/hadoop/kafka_realoutput/pageinfo/date=2016-04-29
34.0 G   hdfs://nameservice1/user/hadoop/kafka_realoutput/pageinfo/date=2016-08-27
33.0 G   hdfs://nameservice1/user/hadoop/kafka_realoutput/pageinfo/date=2016-08-28
6.6 G    hdfs://nameservice1/user/hadoop/kafka_realoutput/pageinfo/date=2016-08-29

# 上传
hadoop fs -put /home/hadoop/users/gongzi/realtime-souredata-1.0-SNAPSHOT-jar-with-dependencies.jar /user/hadoop/spark-jobs/gongzi/

# 删除
hadoop fs -rm hdfs://nameservice1/user/hadoop/spark-jobs/gongzi/realtime-souredata-1.0-SNAPSHOT-jar-with-dependencies.jar

hadoop fs -rm hdfs://nameservice1/user/hadoop/spark-jobs/gongzi/bi-dw-gongzi-realtime.sh
```

#### PathList Test
```
scp hadoop-mapreduce-client-common/2.5.0-cdh5.2.0/hadoop-mapreduce-client-common-cdh5.2.0.jar hadoop@spark001.jp:/home/hadoop/users/gongzi/pl_libs/
scp hadoop-mapreduce-client-common/2.5.0-cdh5.2.0/hadoop-mapreduce-client-common-2.5.0-cdh5.2.0.jar hadoop@spark001.jp:/home/hadoop/users/gongzi/pl_libs/

cd ~/.m2/repository/org/apache/hadoop/
scp hadoop-mapreduce-client-core/2.5.0-cdh5.2.0/hadoop-mapreduce-client-core-2.5.0-cdh5.2.0.jar hadoop@spark001.jp:/home/hadoop/users/gongzi/pl_libs/

scp hadoop-mapreduce-client-shuffle/2.5.0-cdh5.2.0/hadoop-mapreduce-client-shuffle-2.5.0-cdh5.2.0.jar hadoop@spark001.jp:/home/hadoop/users/gongzi/pl_libs/

hadoop-common
scp hadoop-common/2.5.0-cdh5.2.0/hadoop-common-2.5.0-cdh5.2.0.jar hadoop@spark001.jp:/home/hadoop/users/gongzi/pl_libs/

hadoop jar ./pathlist-1.0-SNAPSHOT-jar-with-dependencies.jar com.juanpi.bi.mapred.PathListNew
yarn jar ./pathlist-1.0-SNAPSHOT-jar-with-dependencies.jar com.juanpi.bi.mapred.PathListNew 2016-08-21

# 文件结果
hadoop fs -ls hdfs://nameservice1/user/hadoop/gongzi/dw_real_path_list/date=2016-08-21

```

#### kill spark 作业
```
    ## 查看数据
    hadoop fs -ls hdfs://nameservice1/user/hadoop/gongzi/dw_real_for_path_list/mb_event_hash2/date=2016-08-09/gu_hash=f/
    hadoop fs -tail hdfs://nameservice1/user/hadoop/gongzi/dw_real_for_path_list/mb_event_hash2/date=2016-08-09/gu_hash=f/event1470992520000-r-00011

    hadoop fs -copyToLocal /user/hadoop/gongzi/kafka_realoutput/mb_pageinfo_hash2/date=2016-07-21/hour=9/1469063340000-r-00000 /home/hadoop/users/gongzi/1469063340000.txt

    ## 删除数据
    hadoop fs -rmr /user/hadoop/gongzi/kafka_realoutput/mb_pageinfo_hash2/date=2016-07-19/hour=9/

    org.apache.spark.deploy.Client kill spark://GZ-JSQ-JP-BI-SPARK-001.jp:6066,GZ-JSQ-JP-BI-SPARK-002.jp:6066 <driver ID>
```

##### Kafka Topic：
- pageinfo：mb_pageinfo_hash2
- event：mb_event_hash2
> 当同时消费这两个topic、且结果写同一个目录时，数据会出现异常。测试的时候，数据都写dw_real_for_path_list，当出现写数据延迟，系统会自动创建临时目录：dw_real_for_path_list/_temporary/0/task_201608121541_0130_r_000006，此时两个解析程序就会产生同名的临时文件，会导致两个程序读写文件冲突而退出。

##### 集群模式：pageinfo 测试脚本-V-new bi-dw-gongzi-realtime.sh

### master_event_bi-dw-gongzi.sh
```
#!/usr/bin/env bash

#params=("$@")

echo "gongzi parse mb_pageinfo_hash2 com.juanpi.bi.streaming.KafkaConsumer start..."

/data/apache_projects/spark-hadoop-2.4.0/bin/spark-submit \
    --class com.juanpi.bi.streaming.KafkaConsumer \
    --master spark://GZ-JSQ-JP-BI-SPARK-001.jp:6066,GZ-JSQ-JP-BI-SPARK-002.jp:6066 \
    --deploy-mode cluster \
    --driver-memory 4g \
    --executor-memory 8g \
    --executor-cores 1 \
    --total-executor-cores 12 \
    --conf "spark.default.parallelism=12" \
    --driver-java-options "-XX:PermSize=1024M -XX:MaxPermSize=3072M -Xmx4096M -Xms2048M -Xmn1024M" \
    hdfs://nameservice1/user/hadoop/spark-jobs/gongzi/realtime-souredata-1.0-SNAPSHOT-jar-with-dependencies.jar "zkQuorum"="GZ-JSQ-JP-BI-KAFKA-001.jp:2181,GZ-JSQ-JP-BI-KAFKA-002.jp:2181,GZ-JSQ-JP-BI-KAFKA-003.jp:2181,GZ-JSQ-JP-BI-KAFKA-004.jp:2181,GZ-JSQ-JP-BI-KAFKA-005.jp:2181" "brokerList"="kafka-broker-000.jp:9082,kafka-broker-001.jp:9083,kafka-broker-002.jp:9084,kafka-broker-003.jp:9085,kafka-broker-004.jp:9086,kafka-broker-005.jp:9087,kafka-broker-006.jp:9092,kafka-broker-007.jp:9093,kafka-broker-008.jp:9094,kafka-broker-009.jp:9095,kafka-broker-010.jp:9096,kafka-broker-011.jp:9097" "topic"="mb_event_hash2" "groupId"="bi_gongzi_mb_event_real_direct_by_dw" "consumerType"=1 "consumerTime"=60

if test $? -ne 0
then
echo "spark failed!"
exit 2
fi

```

##### client模式：Event 测试脚本 bi-dw-gongzi-realtime.sh

```
#!/usr/bin/env bash

#params=("$@")

echo "gongzi parse mb_event_hash2 com.juanpi.bi.streaming.KafkaConsumer start..."

/data/apache_projects/spark-hadoop-2.4.0/bin/spark-submit \
    --class com.juanpi.bi.streaming.KafkaConsumer \
    --master spark://GZ-JSQ-JP-BI-SPARK-002.jp:7077,GZ-JSQ-JP-BI-SPARK-001.jp:7077 \
    --deploy-mode client \
    --driver-memory 4g \
    --executor-memory 4g \
    --executor-cores 2 \
    --total-executor-cores 12 \
    --conf "spark.default.parallelism=24" \
    --driver-java-options "-XX:PermSize=1024M -XX:MaxPermSize=3072M -Xmx4096M -Xms2048M -Xmn1024M" \
    /home/hadoop/users/gongzi/realtime-souredata-1.0-SNAPSHOT-jar-with-dependencies.jar "zkQuorum"="GZ-JSQ-JP-BI-KAFKA-001.jp:2181,GZ-JSQ-JP-BI-KAFKA-002.jp:2181,GZ-JSQ-JP-BI-KAFKA-003.jp:2181,GZ-JSQ-JP-BI-KAFKA-004.jp:2181,GZ-JSQ-JP-BI-KAFKA-005.jp:2181" "brokerList"="kafka-broker-000.jp:9082,kafka-broker-001.jp:9083,kafka-broker-002.jp:9084,kafka-broker-003.jp:9085,kafka-broker-004.jp:9086,kafka-broker-005.jp:9087,kafka-broker-006.jp:9092,kafka-broker-007.jp:9093,kafka-broker-008.jp:9094,kafka-broker-009.jp:9095,kafka-broker-010.jp:9096,kafka-broker-011.jp:9097" "topic"="mb_event_hash2" "groupId"="bi_mb_event_real_direct_by_dw" "consumerType"=1 "consumerTime"=5

if test $? -ne 0
then
echo "spark failed!"
exit 2
fi
```

hadoop fs -rm hdfs://nameservice1/user/hadoop/spark-jobs/gongzi/realtime-souredata-1.0-SNAPSHOT-jar-with-dependencies.jar
hadoop fs -put /home/hadoop/users/gongzi/realtime-souredata-1.0-SNAPSHOT-jar-with-dependencies.jar /user/hadoop/spark-jobs/gongzi/

##### 上线脚本-V1 bi-dw-gongzi-realtime.sh
```
#!/usr/bin/env bash

#params=("$@")

/data/apache_projects/spark-hadoop-2.4.0/bin/spark-submit \
    --class com.juanpi.bi.streaming.KafkaConsumer \
    --master spark://GZ-JSQ-JP-BI-SPARK-001.jp:6066,GZ-JSQ-JP-BI-SPARK-002.jp:6066 \
    --deploy-mode cluster \
    --driver-memory 4g \
    --executor-memory 8g \
    --executor-cores 1 \
    --total-executor-cores 4 \
    --conf "spark.default.parallelism=4" \
    --driver-java-options "-XX:PermSize=1024M -XX:MaxPermSize=3072M -Xmx4096M -Xms2048M -Xmn1024M" \
    hdfs://nameservice1/user/hadoop/spark-jobs/gongzi/realtime-souredata-1.0-SNAPSHOT-jar-with-dependencies.jar "zkQuorum"="GZ-JSQ-JP-BI-KAFKA-001.jp:2181,GZ-JSQ-JP-BI-KAFKA-002.jp:2181,GZ-JSQ-JP-BI-KAFKA-003.jp:2181,GZ-JSQ-JP-BI-KAFKA-004.jp:2181,GZ-JSQ-JP-BI-KAFKA-005.jp:2181" "brokerList"="kafka-broker-000.jp:9082,kafka-broker-001.jp:9083,kafka-broker-002.jp:9084,kafka-broker-003.jp:9085,kafka-broker-004.jp:9086,kafka-broker-005.jp:9087,kafka-broker-006.jp:9092,kafka-broker-007.jp:9093,kafka-broker-008.jp:9094,kafka-broker-009.jp:9095,kafka-broker-010.jp:9096,kafka-broker-011.jp:9097" "topic"="mb_event_hash2" "groupId"="bi_gongzi_mb_event_real_direct_by_dw" "consumerType"=1 "consumerTime"=60

if test $? -ne 0
then
echo "spark failed!"
exit 2
fi

```

#### 解析文件输出路径
```
select substring(gu_id, length(gu_id)), count(gu_id) from dw.fct_session where date = '2016-07-21' group by substring(gu_id, length(gu_id));
```

```
#!/bin/bash
. /home/hadoop/.bash_profile

if [ $# == 1 ]; then
   date=$1
else
   date=`date -d -1days '+%Y-%m-%d'`
fi


THIS="$0"
THIS_DIR=`dirname "$THIS"`
cd ${THIS_DIR}

### 传递空参
yarn jar ./pathlist-1.0-SNAPSHOT-jar-with-dependencies.jar com.juanpi.bi.mapred.PathListNew 2016-08-29

if test $? -ne 0
then
exit 11
fi

```

0 * * * * sh /home/hadoop/users/gongzi/run_pathlist.sh > /home/hadoop/users/gongzi/out_event-real-bi-dw-gongzi.txt 2>&1


nohup ./event-real-bi-dw-gongzi.sh > /home/hadoop/users/gongzi/out_event-real-bi-dw-gongzi.txt 2>&1 &
nohup ./page-real-bi-dw-gongzi.sh > /home/hadoop/users/gongzi/out_page-real-bi-dw-gongzi.txt 2>&1 &

nohup ./reget_event-real-bi-dw-gongzi.sh > /home/hadoop/users/gongzi/out_reget_event-real-bi-dw-gongzi.txt 2>&1 &
nohup ./reget_page-real-bi-dw-gongzi.sh > /home/hadoop/users/gongzi/out_reget_page-real-bi-dw-gongzi.txt 2>&1 &

hadoop fs -du -h hdfs://nameservice1/user/hadoop/gongzi/dw_real_for_path_list/mb_event_hash2/date=2016-08-30
hadoop fs -du -h hdfs://nameservice1/user/hadoop/gongzi/dw_real_for_path_list/mb_pageinfo_hash2/date=2016-08-30

hadoop fs -du -h hdfs://nameservice1/user/hadoop/gongzi/dw_real_path_list/date=2016-08-29
hadoop fs -ls -h hdfs://nameservice1/user/hadoop/gongzi/dw_real_path_list/date=2016-08-30

yarn jar ./pathlist-1.0-SNAPSHOT-jar-with-dependencies.jar com.juanpi.bi.mapred.PathListControledJobs 2016-08-30