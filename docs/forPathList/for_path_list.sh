#!/bin/sh

. /etc/profile

function get_date_daysbefore()
{
    sec=`date -d $1 +%s`
    sec_30daysbefore=$((sec - 86400*1))
    days_before=`date -d @$sec_30daysbefore +%F`
    echo $days_before
}

if [ $# == 1 ]; then
   dt=$1
   beforeDate=`get_date_daysbefore $1`
else
   dt=`date -d -1days '+%Y-%m-%d'`
   beforeDate=`date -d -2days '+%Y-%m-%d'`
fi

curdt=`date '+%Y-%m-%d %H:%M'`
DB="dw"
TABLE="fct_for_path_list"

mergeBegin=$(date +%s)
echo "合并 dw.fct_page_ref_reg + dw.fct_event_event 两张表date=$dt 的数据......"
hiveF ./script/fct_path_list_mapr.sql -date $date
if test $? -ne 0
then
exit 11
fi
mergeEnd=$(date +%s)

echo "合并 dw.fct_page_ref_reg + dw.fct_event_event 两张表date=$dt 的数据完成。Merge 耗时: $(($mergeEnd-$mergeBegin)) 秒"

echo "处理数据开始，日期为：$dt"

THIS="$0"
THIS_DIR=`dirname "$THIS"`
cd ${THIS_DIR}

yarn jar ./pathlist.jar com.juanpi.bi.mapred.OfflinePathList >> /home/hadoop/users/gongzi/jars/out_pathlist_2016-11-10.log 2>&1

if test $? -ne 0
then
exit 11
fi

mapREnd=$(date +%s)
echo "当前时间: $curdt, 处理 OfflinePathList 完成，处理日期为：$dt, MapReduce 耗时: $(($mapREnd-$mergeEnd)) 秒!!!"

echo "-------------------------------------------------------------------------------------------------------"

sqlStr="show partitions $DB.$TABLE partition(date='$dt')"

echo $sqlStr > ./verify_partitions.sql
hive -f ./verify_partitions.sql > verify_partitions.txt

if [ `cat verify_partitions.txt|wc -l` -eq 0 ];then
    echo "=====>> create hive_partitions......"
    hive -d dbName=$DB -d date=$dt -f ./hive_partitions.sql
    echo "当前时间: $curdt, $DB.$TABLE 分区 $dt 创建成功!"
else
    echo "=====>> partitions exist!"
fi

if test $? -ne 0
then
exit 11
fi

echo "=====>> 将hdfs文件load至hive......"
hive  -d dbName=$DB -d tableName=$TABLE -f ./load_to_hive.sql >> ./out_load_to_hive_$dt.log 2>&1

all_tend=$(date +%s)
echo "当前时间: $curdt, 处理 OfflinePathList 全部完成，处理日期为：$dt, all_total耗时: $(($all_tend-$pt_tbegin)) 秒!!!"

if test $? -ne 0
then
exit 11
fi