//package com.juanpi.bi.init
//
//import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor}
//
///**
//  * Created by gongzi on 2016/7/7.
//  */
//class initTicksHistory {
//
//  val userTable = TableName.valueOf("user")
//
//  //创建 user 表
//  val tableDescr = new HTableDescriptor(userTable)
//  tableDescr.addFamily(new HColumnDescriptor("basic".getBytes))
//  println("Creating table `user`. ")
//  if (admin.tableExists(userTable)) {
//    admin.disableTable(userTable)
//    admin.deleteTable(userTable)
//  }
//  admin.createTable(tableDescr)
//  println("Done!")
//
//}
