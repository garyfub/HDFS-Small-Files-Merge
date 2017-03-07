package com.juanpi.bi.transformer

import com.juanpi.bi.sc_utils.DateUtils
import com.juanpi.hive.udf.{GetDwPcPageValue, GetGoodsId, GetPageID}
import play.api.libs.json.Json

/**
  * Created by gongzi on 2016/9/28.
  */
object checkEvent {
//  (for_pageid:page_h5https://tuan.juanpi.com/pintuan/item/36717717, ,page_type_id:0, ,page_level_id:0, ,cid, ,f_page_extend_params:https://tuan.juanpi.com/pintuan/item/36717717, ,pagename:page_h5, ,page_value:https://tuan.juanpi.com/pintuan/item/36717717, ,f_page_extend_params:https://tuan.juanpi.com/pintuan/item/36717717, ,d_page_id:0, ,d_page_value:, ,for_eventid:click_share, ,event_id:497, ,d_event_id:497, ,event_type_id:0, ,cid:, ,activityname:click_share, ,t_extend_params:https://tuan.juanpi.com/pintuanlottery/item/36717717/, ,event_value:https://tuan.juanpi.com/pintuanlottery/item/36717717/)

  val linePage = "{\"activityname\":\"click_share\",\"app_name\":\"zhe\",\"app_version\":\"4.2.2\",\"c_label\":\"C2\",\"c_server\":\"{\\\"gid\\\":\\\"C2\\\",\\\"ugroup\\\":\\\"491_451_580_486_538_485_573_496_517_578_449\\\"}\",\"cube_position\":\"\",\"deviceid\":\"357630056196218\",\"endtime\":\"1483604219816\",\"endtime_origin\":\"1483604219418\",\"extend_params\":\"https://tuan.juanpi.com/pintuanlottery/item/36717717/\",\"ip\":\"221.206.145.223\",\"jpid\":\"ffffffff-8d79-be98-cc72-9cfe62cce3ff\",\"location\":\"黑龙江省牡丹江市宁安市宁马线靠近小北沟村\",\"os\":\"android\",\"os_version\":\"5.0\",\"page_extends_param\":\"https://tuan.juanpi.com/pintuan/item/36717717\",\"pagename\":\"page_h5\",\"pre_extends_param\":\"https://tuan.juanpi.com/pintuan/item/36717717\",\"pre_page\":\"page_h5\",\"result\":\"1\",\"server_jsonstr\":\"{}\",\"session_id\":\"1461985086574_zhe_1483604071977\",\"source\":\"\",\"starttime\":\"1483604219816\",\"starttime_origin\":\"1483604219418\",\"ticks\":\"1461985086574\",\"to_switch\":\"0\",\"uid\":\"27653136\",\"utm\":\"101218\"}"

  val lineEvent = "{\"activityname\":\"click_cube_goods\",\"app_name\":\"zhe\",\"app_version\":\"4.2.2\",\"c_label\":\"C2\",\"c_server\":\"{\\\"gid\\\":\\\"C2\\\",\\\"ugroup\\\":\\\"581_486_485_478_496_337_453_493_447_457_377_574_547_584_572_593\\\"}\",\"cube_position\":\"\",\"deviceid\":\"861463039027477\",\"endtime\":\"1483669710474\",\"endtime_origin\":\"1483669703060\",\"extend_params\":\"\",\"ip\":\"112.96.109.136\",\"jpid\":\"00000000-72f7-1057-3187-cb37480a43f3\",\"location\":\"广东省广州市花都区清莲路靠近广州金明进出口有限公司\",\"os\":\"android\",\"os_version\":\"5.1\",\"page_extends_param\":\"313\",\"pagename\":\"page_tab\",\"pre_extends_param\":\"https://m.juanpi.com/zhuanti/xbsdhh?qminkview=1&qmshareview=1\",\"pre_page\":\"page_h5\",\"result\":\"1\",\"server_jsonstr\":\"{\\\"pit_info\\\":\\\"brand::1893518::1_5\\\",\\\"hot_goods_id\\\":\\\"38030687\\\",\\\"cid\\\":313,\\\"_t\\\":1483669188,\\\"_gsort_key\\\":\\\"GSORT2_SERVICE_POSTION_SORT_175_20170106_10_261_410_175586efd3561c4c\\\",\\\"_pit_type\\\":6,\\\"_z\\\":\\\"5\\\"}\",\"session_id\":\"1462589463756_zhe_1483669164198\",\"source\":\"\",\"starttime\":\"1483669710474\",\"starttime_origin\":\"1483669703060\",\"ticks\":\"1462589463756\",\"to_switch\":\"0\",\"uid\":\"31215985\",\"utm\":\"101221\"}"

  def testcid(server_jsonstr: String): Any = {
    if (server_jsonstr.contains("cid")) {
      val js_server_jsonstr = Json.parse(server_jsonstr)
      val cid = (js_server_jsonstr \ "cid").asOpt[String].getOrElse("")
      println(cid)
    }
  }

  def getDateFilter(groupId: String) ={
    // 当前日期
    val endDateStr = DateUtils.getDateMinusDays(0)

    val startDateStr = if(groupId.startsWith("re")) {
      // 重新消费的话，groupID必定是re开头
      DateUtils.getDateMinusDays(6)
    } else {
      // 否则就是当下的日期
      endDateStr
    }

    val starttime_origin = "1488230401592"

    // 如果从日志解析得到的时间不是当前消费的日期，就将该数据过滤掉
    val dateStr = DateUtils.dateStr(starttime_origin.toLong)
    println(dateStr)
    // 如果日志时间超出了范围，就过滤掉
    if(dateStr < startDateStr || dateStr > endDateStr){
      println("log时间不在合理区间")
    }
  }

  def eventParse(): Unit = {
    val pp = new GetDwPcPageValue()
    val url = "https://m.juanpi.com/zhuanti/lstop?mobile=1&qminkview=1&qmshareview=1"
    val value = pp.evaluate(url)
    println(value)
    // 失败
    val pid = new GetPageID().evaluate("http://m.juanpi.com/shop/28334714").toInt
    println(pid)

    val page_id = new GetGoodsId().evaluate("28334714")
    println(page_id)

    val (sortdate, sorthour, lplid, ptplid) = eventParser.getGsortKey("GSORT2_SERVICE_POSTION_SORT_161_20161027_20_169_204_1615811fab82b2ef")
    println(sortdate, sorthour, lplid, ptplid)

    //    val (test_id,select_id) = eventParser.getAbinfo("{\"pit_info\":\"goods::37296487::1_1\",\"ab_info\":\"B72\",\"cid\":-1,\"_t\":1477560627,\"_gsort_key\":\"DEFAULT_SORT_85_20161027_16_105_8558119705838c5\",\"_pit_type\":3}")

    val abInfo = """{"pit_info":"goods::21695951::4_12","ab_info":"A89","cid":-4,"_t":1478078104,"_gsort_key":"GSORT2_SERVICE_POSTION_SORT_41_20161102_16_45_110_415819ab52b6877","_pit_type":3}"""
    val (test_id,select_id) = eventParser.getAbinfo(abInfo)

    println(test_id,select_id)
  }

  def testDateGuidPartitions() {
    val timeStamp = 1482803480
    val gu_id = ""
    val partitionStr = DateUtils.dateGuidPartitions(timeStamp, gu_id)
    println(partitionStr)
  }

  def checkLinePage(): Unit = {
//    val res = eventParser.filterFunc(linePage)

//    val row = Json.parse(linePage)
    val row = Json.parse(lineEvent)

//    val session_id = (row \ "session_id").asOpt[String].getOrElse("")
    val activityname = (row \ "activityname").asOpt[String].getOrElse("").toLowerCase()

    //    修正过的时间
//    val startTime = (row \ "starttime_origin").asOpt[String].getOrElse("")
//    val endtime_origin = (row \ "endtime_origin").asOpt[String].getOrElse("")
//    val endTime = pageAndEventParser.getEndTime(startTime, endtime_origin)
//
//    //    val result = (row \ "result").asOpt[String].getOrElse("")
//    val uid = (row \ "uid").asOpt[String].getOrElse("0")
    val extend_params = (row \ "extend_params").asOpt[String].getOrElse("")
    // utm 的值还会改变，故定义成var
//    val utm = (row \ "utm").asOpt[String].getOrElse("")
//    val source = ""
//
//    val app_name = (row \ "app_name").asOpt[String].getOrElse("")
    val app_version = (row \ "app_version").asOpt[String].getOrElse("0")
//    val os = (row \ "os").asOpt[String].getOrElse("")
    val pagename = (row \ "pagename").asOpt[String].getOrElse("").toLowerCase()
    val page_extends_param = (row \ "page_extends_param").asOpt[String].getOrElse("")
//    val deviceid = (row \ "deviceid").asOpt[String].getOrElse("")
    val pre_page = (row \ "pre_page").asOpt[String].getOrElse("")
//    // 字段与pageinfo中的不太一样
    val pre_extends_param = (row \ "pre_extends_param").asOpt[String].getOrElse("")
//    val jpid = (row \ "jpid").asOpt[String].getOrElse("")
//    val ip = ""
//    val to_switch = (row \ "to_switch").asOpt[String].getOrElse("")
    val cube_position = (row \ "cube_position").asOpt[String].getOrElse("")
//    val location = (row \ "location").asOpt[String].getOrElse("")
//    val ctag = (row \ "c_label").asOpt[String].getOrElse("")
    val server_jsonstr = (row \ "server_jsonstr").asOpt[String].getOrElse("")

//    val loadTime = pageAndEventParser.getJsonValueByKey(server_jsonstr, "_t")
//    val ug_id = pageAndEventParser.getJsonValueByKey(server_jsonstr, "_z")

    // ------------------------------------------------------------- mb_event -> mb_event_log --------------------------------------------------------------
    val f_page_extend_params = pageAndEventParser.getExtendParams(pagename, page_extends_param)
    val f_pre_extend_params = pageAndEventParser.getExtendParams(pagename, pre_extends_param)

    val t_extend_params = eventParser.getExtendParamsFromBase(activityname, extend_params, app_version)
//    val f_extend_params = eventParser.getForExtendParams(activityname, t_extend_params, cube_position, server_jsonstr)
    val cid = pageAndEventParser.getJsonValueByKey(server_jsonstr, "cid")

    val forPageId = eventParser.getForPageId(cid, f_page_extend_params, pagename)

    val forPrePageid = eventParser.getForPrePageId(pagename, f_pre_extend_params, pre_page)

    val forEventId = eventParser.getForEventId(cid, activityname, t_extend_params)

    println("")
  }

  def main(args: Array[String]): Unit = {
//    testDateGuidPartitions()
    getDateFilter("reprod_test_bi_realtime_by_dw_mb_event_hash2")
  }
}
