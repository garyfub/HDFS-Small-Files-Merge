#!/bin/sh

. /etc/profile

if [ $# == 1 ]; then
    today=$1
else
    today=`date '+%Y-%m-%d'`
    ## 当前时间
    curdt=`date '+%Y-%m-%d %H:%M'`
    curhour=`date '+%H'`
fi

DB="dw"
TABLE="fct_for_path_list"

echo "处理数据开始，日期为：$today"

THIS="$0"
THIS_DIR=`dirname "$THIS"`
cd ${THIS_DIR}

pt_tbegin=$(date +%s)

yarn jar /home/hadoop/users/gongzi/jars/pathlist.jar com.juanpi.bi.mapred.OfflinePathList >> /home/hadoop/users/gongzi/jars/out_pathlist_2016-11-10.log 2>&1

if test $? -ne 0
then
exit 11
fi

pt_tend=$(date +%s)
echo "当前时间: $curdt, 处理 OfflinePathList 完成，处理日期为：$today, total耗时: $(($pt_tend-$pt_tbegin)) 秒!!!"

echo "-------------------------------------------------------------------------------------------------------"

hive  -d dbName=$DB -d tableName=$TABLE -f ./load_to_hive.sql >> ./out_load_to_hive_$today.log 2>&1

all_tend=$(date +%s)
echo "当前时间: $curdt, 处理 OfflinePathList 全部完成，处理日期为：$today, all_total耗时: $(($all_tend-$pt_tbegin)) 秒!!!"

if test $? -ne 0
then
exit 11
fi