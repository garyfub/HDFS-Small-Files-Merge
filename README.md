小文件合并使用说明
====
[TOC]

## 项目git地址
```bash
https://gitlab.juanpi.org/bi-source/dw-realtime.git
```

## 文件目录
```bash
hdfs-files-merge
    docs
    profiles
    src
        main
            java
                com
                    juanpi
                        bi
                            merge
                                utils
            resources
```

## Jenkins 持续集成
> http://jenkins.juanpi.org/jenkins/job/hdfs-files-merge

## 测试
```
## jenkins 打包命令, 读取配置文件 /profiles/test_config.properties 文件
mvn clean clean package -DskipTests -P test -pl hdfs-files-merge

```

## 合并重新消费的文件
```
## jenkins 打包命令, 读取配置文件 /profiles/reprod_config.properties 文件
mvn clean clean package -DskipTests -P reprod -pl hdfs-files-merge

## 重新消费数据路径
hadoop fs -du -h hdfs://nameservice1/user/hadoop/dw_realtime/reprod/mb_event_hash2/
```

## 合并实时消费的文件
``` sh
## jenkins 打包命令, 读取配置文件 /profiles/prod_config.properties 文件
mvn clean package -DskipTests -P prod -pl hdfs-files-merge

## 实时消费kafka后产生的数据文件，
hadoop fs -du -h hdfs://nameservice1/user/hadoop/dw_realtime/dw_real_for_path_list/

```

## 在集群上运行
``` sh
## 申请ops001的权限，将jenkins的包复制到spark集群所在的机器
scp /data/jenkins_workspace/workspace/hdfs-files-merge/hdfs-files-merge/target/hdfs-files-merge-1.0.jar hadoop@spark001.jp:/home/hadoop/users/gongzi/hdfs_smallfiles/

## 需要spark001的权限，登陆到spark001
cd /home/hadoop/users/gongzi/hdfs_smallfiles/

## 清理历史文件
## 清理25号之前7天的数据
yarn jar ./hdfs-files-merge-1.0.jar com.juanpi.bi.merge.CleanHistoricalData 2017-02-25

## 清理小文件,日期根据实际情况传递
yarn jar ./hdfs-files-merge-1.0.jar com.juanpi.bi.merge.TaskManager 2017-03-14
```