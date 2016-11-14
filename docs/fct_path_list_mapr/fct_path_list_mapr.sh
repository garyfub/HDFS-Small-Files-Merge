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
   date=$1
   beforeDate=`get_date_daysbefore $1`
else
   date=`date -d -1days '+%Y-%m-%d'`
   beforeDate=`date -d -2days '+%Y-%m-%d'`
fi

THIS="$0"
THIS_DIR=`dirname "$THIS"`
cd ${THIS_DIR}

hiveF ./script/fct_path_list_mapr.sql -date $date
if test $? -ne 0
then
exit 11
fi
