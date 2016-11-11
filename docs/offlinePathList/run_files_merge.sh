#!/bin/sh

. /etc/profile

if [ $# == 1 ]; then
    today=$1
else
    # today=`date -d -1days '+%Y-%m-%d'`
    today=`date '+%Y-%m-%d'`
    ## 当前时间
    curdt=`date '+%Y-%m-%d %H:%M'`
    curhour=`date '+%H'`
fi

DB="dw"
TABLE="path_list_real"

if [ $curhour == "00" ]; then
    echo "当前小时为$curhour, 刚过零点，创建$today这一天的分区"
    ## 预创建当天的分区，比如,2016-09-27 00:01:00,刚刚到这一天，创建这一天的分区
    hive -d dbName=$DB -d date=$today -f ./hive_partitions.sql
    if test $? -ne 0
    then
    exit 11
    fi
    ## 零点，处理前一天的数据, 比如，2016-09-27 00:01:00,刚刚到这一天，需要处理 2016-09-26 23点~00点的数据
    today=`date -d -1days '+%Y-%m-%d'`
fi

echo "处理数据开始，日期为：$today"

THIS="$0"
THIS_DIR=`dirname "$THIS"`
cd ${THIS_DIR}

fm_tbegin=$(date +%s)
### 传递空参
yarn jar ./hdfs-files-merge.jar  "$today" >> ./out_filemerge_$today.log 2>&1

fm_tend=$(date +%s)
echo "当前时间: $curdt, 合并小文件完毕. 处理日期为：$today, total耗时: $(($fm_tend-$fm_tbegin)) 秒!!!"

if test $? -ne 0
then
exit 11
fi

echo "-------------------------------------------------------------------------------------------------------"

pt_tbegin=$(date +%s)

yarn jar ./pathlist-jar-with-dependencies.jar com.juanpi.bi.mapred.PathListControledJobs "$today" >> ./out_pathlist_$today.log 2>&1

yarn jar /home/hadoop/users/gongzi/jars/pathlist.jar com.juanpi.bi.mapred.OfflinePathList >> /home/hadoop/users/gongzi/jars/out_pathlist_2016-11-10.log 2>&1

if test $? -ne 0
then
exit 11
fi

pt_tend=$(date +%s)
echo "当前时间: $curdt, 处理 PathListNew 完成，处理日期为：$today, total耗时: $(($pt_tend-$pt_tbegin)) 秒!!!"

echo "-------------------------------------------------------------------------------------------------------"


hive  -d dbName=$DB -d tableName=$TABLE -d date=$today -f ./load_to_hive.sql >> ./out_load_to_hive_$today.log 2>&1

all_tend=$(date +%s)
echo "当前时间: $curdt, 处理 PathListNew 全部完成，处理日期为：$today, all_total耗时: $(($all_tend-$fm_tbegin)) 秒!!!"

if test $? -ne 0
then
exit 11
fi