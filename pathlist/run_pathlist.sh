#!/bin/sh

. /etc/profile

if [ $# == 1 ]; then
   date=$1
else
   date=`date -d -1days '+%Y-%m-%d'`
fi


THIS="$0"
THIS_DIR=`dirname "$THIS"`
cd ${THIS_DIR}

### 传递空参
yarn jar ./pathlist-1.0-SNAPSHOT-jar-with-dependencies.jar com.juanpi.bi.mapred.PathListNew ""

if test $? -ne 0
then
exit 11
fi