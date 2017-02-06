package com.juanpi.bi.transformer

import com.juanpi.bi.sc_utils.DateUtils
import com.juanpi.hive.udf.GetPageID
import play.api.libs.json.Json

/**
  * Created by gongzi on 2016/9/30.
  */
object checkPage {

  val line = "{\"app_name\":\"jiu\",\"app_version\":\"4.2.1\",\"c_label\":\"C2\",\"c_server\":\"{\\\"gid\\\":\\\"C2\\\",\\\"ugroup\\\":\\\"580_643_486_644_485_631_496_630_453_491_492_457_622_377_627_572_517_629_602\\\"}\",\"deviceid\":\"359901057800191\",\"endtime\":\"1484813956731\",\"endtime_origin\":\"1484813956065\",\"extend_params\":\"32612735\",\"ip\":\"180.142.222.146\",\"jpid\":\"ffffffff-f9ba-487d-367f-8a000da939b3\",\"location\":\"广西壮族自治区玉林市兴业县玉贵路靠近兴业县妇幼保健院\",\"os\":\"android\",\"os_version\":\"4.3\",\"pagename\":\"page_temai_goods\",\"pre_extend_params\":\"all\",\"pre_page\":\"page_tab\",\"server_jsonstr\":\"{}\",\"session_id\":\"1475993842938_jiu_1484812874497\",\"source\":\"\",\"starttime\":\"1484813949434\",\"starttime_origin\":\"1484813948768\",\"ticks\":\"1475993842938\",\"to_switch\":\"1\",\"uid\":\"29932735\",\"utm\":\"101212\",\"wap_pre_url\":\"\",\"wap_url\":\"\"}"

  def testcid(server_jsonstr: String): Any = {
    if (server_jsonstr.contains("cid")) {
      val js_server_jsonstr = Json.parse(server_jsonstr)
      val cid = (js_server_jsonstr \ "cid").asOpt[String].getOrElse("")
      println(cid)
    }
  }

  def main(args: Array[String]): Unit = {

//    val for_pageid="page_temai_goods"
//    val page_type_id=8
//    val page_level_id="0"
//    val pageName="page_temai_goods"
//    val page_value="null"
//    val fct_extendParams="4497809"
//    val d_page_id=158
//    val d_page_value="goods_id"
//    val res = pageParser.getPageValue(d_page_id, fct_extendParams, "", page_type_id, d_page_value)
//
//    println(res)

    val pid = -999: java.lang.Integer
    println(pid.toInt)


//    val row = Json.parse(line.replaceAll("null", """\\"\\""""))
//    val starttime_origin = (row \ "starttime_origin").asOpt[String].getOrElse("")
//
//    if(starttime_origin.isEmpty) {
//      println("", "", null)
//    }
//
//    val originDateStr = DateUtils.dateStr(starttime_origin.toLong)
//
//    val sDate = DateUtils.getWeekAgoDateStr()
//    val eDate = DateUtils.getWeekLaterDateStr()
//
//    val startTime = if(originDateStr > sDate && originDateStr < eDate) {
//      starttime_origin
//    } else {
//      ""
//    }

  }
}
