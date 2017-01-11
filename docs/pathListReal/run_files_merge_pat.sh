#!/bin/sh

. /etc/profile
. ~/.bash_profile

if [ $# == 1 ]; then
    today=$1
else
    today=`date '+%Y-%m-%d'`
    curhour=`date '+%H'`
    curdt=`date '+%Y-%m-%d %H:%M'`
fi

THIS="$0"
THIS_DIR=`dirname "$THIS"`
cd ${THIS_DIR}

DB="dw"
TABLE="path_list_real"

if [ $curhour == "11" ]; then
    newDay=`date -d +1days '+%Y-%m-%d'`
    echo "当前时间为=> $curdt, 提前一天创建 $newDay 这一天的 hive 分区"
    hive -d dbName=$DB -d date=$newDay -f ./new_partitions.sql
    if test $? -ne 0
    then
        exit 11
    fi
fi

## 零点，处理前一天的数据, 比如，2016-09-27 00:01:00, 刚刚到这一天，需要处理 2016-09-26 23点~00点的数据
if [ $curhour == "00" ]; then
    today=`date -d -1days '+%Y-%m-%d'`
    echo "当前时间: $curdt ，处理 $today 23点~0点 的数据"
fi

echo "当前时间为=> $curdt, 处理数据开始..."

fm_tbegin=$(date +%s)
yarn jar ./hdfs-files-merge-1.0.jar "$today"

fm_tend=$(date +%s)
echo "当前时间: $curdt, 合并小文件完毕. 处理日期为：$today, total耗时: $(($fm_tend-$fm_tbegin)) 秒!!!"

if test $? -ne 0
then
exit 11
fi

echo "-------------------------------------------------------------------------------------------------------"

pt_tbegin=$(date +%s)
yarn jar ./pathlist-1.0.jar com.juanpi.bi.mapred.PathListControledJobs "$today"
if test $? -ne 0
then
exit 11
fi

pt_tend=$(date +%s)
echo "当前时间: $curdt, PathList 路径计算完成，处理日期为：$today, total耗时: $(($pt_tend-$pt_tbegin)) 秒!!!"

echo "-------------------------------------------------------------------------------------------------------"


hive  -d dbName=$DB -d tableName=$TABLE -d date=$today -f ./load_to_hive.sql

all_tend=$(date +%s)
echo "当前时间: $curdt, load_to_hive 处理完成。PathList 相关任务全部完成，处理日期为：$today, all_total耗时: $(($all_tend-$fm_tbegin)) 秒!!!"

if test $? -ne 0
then
exit 11
fi
