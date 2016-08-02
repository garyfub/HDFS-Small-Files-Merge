package com.juanpi.bi.sc_utils

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
 * Created by juanpi on 2015/8/31.
 */
object SparkSQLExec {

  def getAllSql(jarfile:String,commands:String) = {
    val file = Source.fromFile(jarfile)
    val udfSql = file.mkString

    val allSql = udfSql+commands

    allSql.split(";")
  }

  def run(sqlCommands:Seq[String]) = {
    val sparkConf = new SparkConf().setAppName("SparkSQLExec")
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)

    sqlCommands.foreach{ sql =>
      if(sql.trim().length>0) {
        hiveContext.sql(sql)
      }
    }
    sc.stop()
  }


  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println( s"""
                            |Usage: SparkSQLExec <commands>
                            |  <commands> sql commands
                            |
        """.stripMargin)
      System.exit(1)
    }

    val commands = args(0)

    //val allSql = getAllSql(jarfile,commands)
    run(commands.split(";"))
  }
}
