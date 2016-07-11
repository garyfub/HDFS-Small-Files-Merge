//package com.juanpi.bi.hbase
//
//import org.apache.hadoop.hbase.client.{Connection, Get, Put, Scan}
//import org.apache.hadoop.hbase.util.Bytes
//import org.apache.zookeeper.Op.Delete
//
///**
//  * Created by gongzi on 2016/7/7.
//  */
//class HbaseCRUD {
//
//  def test(conn: Connection, table: String): Unit = {
//    try{
//      //获取 user 表
//      val table = conn.getTable(table)
//
//      try{
//        //准备插入一条 key 为 id001 的数据
//        val p = new Put("id001".getBytes)
//        //为put操作指定 column 和 value （以前的 put.add 方法被弃用了）
//        p.addColumn("basic".getBytes,"name".getBytes, "wuchong".getBytes)
//        //提交
//        table.put(p)
//
//        //查询某条数据
//        val g = new Get("id001".getBytes)
//        val result = table.get(g)
//        val value = Bytes.toString(result.getValue("basic".getBytes,"name".getBytes))
//        println("GET id001 :"+value)
//
//        //扫描数据
//        val s = new Scan()
//        s.addColumn("basic".getBytes,"name".getBytes)
//        val scanner = table.getScanner(s)
//
//        try{
//          for(r <- scanner){
//            println("Found row: "+r)
//            println("Found value: "+Bytes.toString(
//              r.getValue("basic".getBytes,"name".getBytes)))
//          }
//        }finally {
//          //确保scanner关闭
//          scanner.close()
//        }
//
//        //删除某条数据,操作方式与 Put 类似
//        val d = new Delete("id001".getBytes)
//        d.addColumn("basic".getBytes,"name".getBytes)
//        table.delete(d)
//
//      }finally {
//        if(table != null) table.close()
//      }
//
//    }finally {
//      conn.close()
//    }
//  }
//
//
//}
