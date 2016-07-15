
##### jar包上传
```
hadoop fs -mkdir /user/hadoop/spark-jobs/gongzi
hadoop fs -ls /user/hadoop/spark-jobs/gongzi
hadoop fs -put /home/hadoop/users/gongzi/realtime-souredata-1.0-SNAPSHOT-jar-with-dependencies.jar /user/hadoop/spark-jobs/gongzi/
```

##### 上线脚本-V-new bi-dw-gongzi-realtime.sh
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
    --total-executor-cores 12 \
    --conf "spark.default.parallelism=12" \
    --driver-java-options "-XX:PermSize=1024M -XX:MaxPermSize=3072M -Xmx4096M -Xms2048M -Xmn1024M" \
    hdfs://nameservice1/user/hadoop/spark-jobs/gongzi/realtime-souredata-1.0-SNAPSHOT-jar-with-dependencies.jar GZ-JSQ-JP-BI-KAFKA-001.jp:2181,GZ-JSQ-JP-BI-KAFKA-002.jp:2181,GZ-JSQ-JP-BI-KAFKA-003.jp:2181,GZ-JSQ-JP-BI-KAFKA-004.jp:2181,GZ-JSQ-JP-BI-KAFKA-005.jp:2181 kafka-broker-000.jp:9082,kafka-broker-001.jp:9083,kafka-broker-002.jp:9084,kafka-broker-003.jp:9085,kafka-broker-004.jp:9086,kafka-broker-005.jp:9087,kafka-broker-006.jp:9092,kafka-broker-007.jp:9093,kafka-broker-008.jp:9094,kafka-broker-009.jp:9095,kafka-broker-010.jp:9096,kafka-broker-011.jp:9097 pageinfo pageinfo_direct_dw 1 1

if test $? -ne 0
then
echo "spark failed!"
exit 2
fi

```

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
    --executor-cores 30 \
    --total-executor-cores 90 \
    --conf "spark.default.parallelism=90" \
    --driver-java-options "-XX:PermSize=1024M -XX:MaxPermSize=3072M -Xmx4096M -Xms2048M -Xmn1024M" \
    hdfs://nameservice1/user/hadoop/spark-jobs/gongzi/realtime-souredata-1.0-SNAPSHOT-jar-with-dependencies.jar --zkQuorum GZ-JSQ-JP-BI-KAFKA-001.jp:2181,GZ-JSQ-JP-BI-KAFKA-002.jp:2181,GZ-JSQ-JP-BI-KAFKA-003.jp:2181,GZ-JSQ-JP-BI-KAFKA-004.jp:2181,GZ-JSQ-JP-BI-KAFKA-005.jp:2181 --brokers kafka-broker-000.jp:9082,kafka-broker-001.jp:9083,kafka-broker-002.jp:9084,kafka-broker-003.jp:9085,kafka-broker-004.jp:9086,kafka-broker-005.jp:9087,kafka-broker-006.jp:9092,kafka-broker-007.jp:9093,kafka-broker-008.jp:9094,kafka-broker-009.jp:9095,kafka-broker-010.jp:9096,kafka-broker-011.jp:9097 --topic pageinfo --groupId pageinfo_direct_dw --consumerType 1 --consumerTime 1

if test $? -ne 0
then
echo "spark failed!"
exit 2
fi

```