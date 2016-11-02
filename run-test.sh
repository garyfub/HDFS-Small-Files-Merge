#!/usr/bin/env bash

cd ~/dev_pro/dw-realtime/realtime-sourcedata/

git pull origin dev

mvn clean package

scp ~/dev_pro/dw-realtime/realtime-sourcedata/target/realtime-souredata-1.0-SNAPSHOT-jar-with-dependencies.jar hadoop@spark001.jp:/home/hadoop/users/gongzi/jars/

# pathlist
cd ~/dev_pro/dw-realtime/pathlist/

mvn clean package

scp ~/dev_pro/dw-realtime/pathlist/target/pathlist-jar-with-dependencies.jar hadoop@spark001.jp:/home/hadoop/users/gongzi/jars/

# hdfs-files-merge
cd ~/dev_pro/dw-realtime/hdfs-files-merge/

mvn clean package

scp ~/dev_pro/dw-realtime/hdfs-files-merge/target/hdfs-files-merge.jar hadoop@spark001.jp:/home/hadoop/users/gongzi/jars/

if test $? -ne 0
then
echo "spark failed!"
exit 2
fi
