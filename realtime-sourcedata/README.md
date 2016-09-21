
##### jar包上传
```
### on ops001.jp
### 删除 ops001.jp 和 sparkoo1.jp 上的 realtime-souredata-1.0-SNAPSHOT-jar-with-dependencies.jar 后
scp /data/home/gongzi/realtime-souredata-1.0-SNAPSHOT-jar-with-dependencies.jar hadoop@spark001.jp:/home/hadoop/users/gongzi/

scp ~/dev_pro/dw-realtime/realtime-sourcedata/target/realtime-souredata-1.0-SNAPSHOT-jar-with-dependencies.jar hadoop@spark001.jp:/home/hadoop/users/gongzi/

ssh hadoop@spark001.jp
# on sparkoo1.jp

hadoop fs -mkdir /user/hadoop/spark-jobs/gongzi

hadoop fs -du -h hdfs://nameservice1/user/hadoop/gongzi/dw_real_for_path_list/mb_event_hash2/date=2016-08-30
hadoop fs -du -h hdfs://nameservice1/user/hadoop/gongzi/dw_real_for_path_list/mb_pageinfo_hash2/date=2016-08-30

#### 架构做的实时解析的数据参考
[hadoop@GZ-JSQ-JP-BI-SPARK-001 gongzi]$   hadoop fs -du -h hdfs://nameservice1/user/hadoop/kafka_realoutput/mbevent
24.1 G   hdfs://nameservice1/user/hadoop/kafka_realoutput/mbevent/date=2016-08-27
21.8 G   hdfs://nameservice1/user/hadoop/kafka_realoutput/mbevent/date=2016-08-28
4.2 G    hdfs://nameservice1/user/hadoop/kafka_realoutput/mbevent/date=2016-08-29

[hadoop@GZ-JSQ-JP-BI-SPARK-001 gongzi]$   hadoop fs -du -h hdfs://nameservice1/user/hadoop/kafka_realoutput/pageinfo
34.0 G   hdfs://nameservice1/user/hadoop/kafka_realoutput/pageinfo/date=2016-08-27
33.0 G   hdfs://nameservice1/user/hadoop/kafka_realoutput/pageinfo/date=2016-08-28

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


hadoop fs -rm hdfs://nameservice1/user/hadoop/spark-jobs/gongzi/realtime-souredata-1.0-SNAPSHOT-jar-with-dependencies.jar
hadoop fs -put /home/hadoop/users/gongzi/realtime-souredata-1.0-SNAPSHOT-jar-with-dependencies.jar /user/hadoop/spark-jobs/gongzi/


### 传递空参
yarn jar ./pathlist-1.0-SNAPSHOT-jar-with-dependencies.jar com.juanpi.bi.mapred.PathListNew 2016-08-30

if test $? -ne 0
then
exit 11
fi

```

0 * * * * sh /home/hadoop/users/gongzi/run_pathlist.sh > /home/hadoop/users/gongzi/out_event-real-bi-dw-gongzi.txt 2>&1


nohup ./spark_event/event-real-bi-dw-gongzi.sh > /home/hadoop/users/gongzi/spark_event/out_event-real-bi-dw-gongzi.txt 2>&1 &

nohup ./spark_page/page-real-bi-dw-gongzi.sh > /home/hadoop/users/gongzi/spark_page/out_page-real-bi-dw-gongzi.txt 2>&1 &

nohup ./reget_event-real-bi-dw-gongzi.sh > /home/hadoop/users/gongzi/out_reget_event-real-bi-dw-gongzi.txt 2>&1 &
nohup ./reget_page-real-bi-dw-gongzi.sh > /home/hadoop/users/gongzi/out_reget_page-real-bi-dw-gongzi.txt 2>&1 &

hadoop fs -du -h hdfs://nameservice1/user/hadoop/gongzi/dw_real_for_path_list/mb_event_hash2/
hadoop fs -du -h hdfs://nameservice1/user/hadoop/gongzi/dw_real_for_path_list/mb_pageinfo_hash2/


yarn jar ./pathlist-jar-with-dependencies.jar com.juanpi.bi.mapred.PathListControledJobs 2016-08-31

group_id:
bi_gongzi_mb_event_real_direct_by_dw
bi_gongzi_mb_pageinfo_real_direct_by_dw

alter table dw_path_list_new partition (date="2016-09-02", gu_hash="0") set location 'hdfs://nameservice1/user/hadoop/gongzi/dw_real_path_list_jobs/date=2016-09-02/gu_hash=0';

## 从 date=2016-09-19 开始，采用新的数据目录
### dw_real_for_path_list 新目录
hadoop fs -du -h hdfs://nameservice1/user/hadoop/dw_realtime/dw_real_for_path_list/mb_event_hash2/date=2016-09-20
hadoop fs -du -h hdfs://nameservice1/user/hadoop/dw_realtime/dw_real_for_path_list/mb_event_hash2/date=2016-09-20/gu_hash=0/logs
hadoop fs -du -h hdfs://nameservice1/user/hadoop/dw_realtime/dw_real_for_path_list/mb_event_hash2/date=2016-09-20/gu_hash=0/merged

hadoop fs -du -h hdfs://nameservice1/user/hadoop/dw_realtime/dw_real_for_path_list/mb_pageinfo_hash2/date=2016-09-20
hadoop fs -du -h hdfs://nameservice1/user/hadoop/dw_realtime/dw_real_for_path_list/mb_pageinfo_hash2/date=2016-09-20/gu_hash=0/logs
hadoop fs -du -h hdfs://nameservice1/user/hadoop/dw_realtime/dw_real_for_path_list/mb_pageinfo_hash2/date=2016-09-20/gu_hash=0/

### dw_real_path_list
hadoop fs -du -h hdfs://nameservice1/user/hadoop/dw_realtime/dw_real_path_list_jobs/date=2016-09-20
hadoop fs -ls hdfs://nameservice1/user/hadoop/dw_realtime/dw_real_path_list_jobs/date=2016-09-20/gu_hash=0