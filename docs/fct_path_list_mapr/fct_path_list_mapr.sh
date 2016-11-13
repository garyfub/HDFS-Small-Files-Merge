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

hiveF ./script/fct_path_list_mapr.sql -date $date
if test $? -ne 0
then
exit 11
fi
