#!/bin/sh

. /etc/profile

if [ $# == 1 ]; then
   today=$1
else
   today=`date -d -1days '+%Y-%m-%d'`
fi


THIS="$0"
THIS_DIR=`dirname "$THIS"`
cd ${THIS_DIR}

### 传递空参
yarn jar ./pathlist-1.0-SNAPSHOT-jar-with-dependencies.jar com.juanpi.bi.mapred.PathListNew "$today"
if test $? -ne 0
then
exit 11
fi

DB="test"
TABLE="dw_path_list_new"
DataPath="gongzi"
hiveF ./load_to_hive.sql -dbName $DB -tableName $TABLE -dataPath $DATAPath -date $today

if test $? -ne 0
then
exit 11
fi