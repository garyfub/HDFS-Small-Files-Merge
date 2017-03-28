## 操作

### 打包
``` sh
mvn clean package -DskipTests -P prod -pl realtime-sourcedata
```


## 线上运行
```
https://gitlab.juanpi.org/bi-source/dw-realtime/blob/bf3ac1f19f24a8242122e717125cb32bc8bde5fc/realtime-sourcedata/docs/spark-prod.md
```

## 线上重新消费Kafka最近7天的数据
```
https://gitlab.juanpi.org/bi-source/dw-realtime/blob/bf3ac1f19f24a8242122e717125cb32bc8bde5fc/realtime-sourcedata/docs/spark-reprod.md
```

## 测试
```
https://gitlab.juanpi.org/bi-source/dw-realtime/blob/bf3ac1f19f24a8242122e717125cb32bc8bde5fc/realtime-sourcedata/docs/spark-test.md
```