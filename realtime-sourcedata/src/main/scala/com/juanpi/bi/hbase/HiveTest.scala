package com.juanpi.bi.hbase

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by gongzi on 2016/7/7.
  */
object HiveTest {

  def main(args: Array[String]) {
    val sconf = new SparkConf().setAppName("HiveTest_Gong").setMaster("local[*]")
    val sct = new SparkContext(sconf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sct)

//    sqlContext.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
//    sqlContext.sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src")

    // Queries are expressed in HiveQL
    sqlContext.sql("FROM dw.dim_city SELECT * ").collect().foreach(println)
  }

}
