package com.juanpi.bi.transformer

import com.juanpi.hive.udf.{GetDwPcPageValue, GetGoodsId, GetPageID}
import play.api.libs.json.Json

/**
  * Created by gongzi on 2016/9/28.
  */
object checkEvent {

  def testcid(server_jsonstr: String): Any = {
    if (server_jsonstr.contains("cid")) {
      val js_server_jsonstr = Json.parse(server_jsonstr)
      val cid = (js_server_jsonstr \ "cid").asOpt[String].getOrElse("")
      println(cid)
    }
  }

  def main(args: Array[String]) {

//    val line = """{"activityname":"click_cube_goods","app_name":"zhe","app_version":"4.1.1","c_label":"C3","c_server":"{\"gid\":\"C3\",\"ugroup\":\"222\"}","cube_position":"1_1","deviceid":"49A3FDC8-2501-4250-8A06-BAF463A008E6","endtime":"1475052337104","endtime_origin":"1475052335999","extend_params":"","ip":"180.156.82.107","jpid":"ba67668beaf7eb8c00cc854dff02e3490b6a890c","location":"","os":"iOS","os_version":"9.3.2","page_extends_param":"345","pagename":"page_tab","pre_extends_param":"316","pre_page":"page_tab","result":"1","server_jsonstr":"{\"pit_info\":\"goods::21761493::1_1\",\"ab_attr\":\"9\",\"cid\":345,\"_t\":1475052281,\"_gsort_key\":\"DEFAULT_SORT_221_20160928_16_276\",\"_pit_type\":3}","session_id":"1448354623610_zhe_1475052328304","source":"","starttime":"1475052337104","starttime_origin":"1475052335999","ticks":"1448354623610","to_switch":"0","uid":"0","utm":"101431"}"""
//
//    //    (for_pageid:page_active, ,page_type_id:5, ,page_level_id:3, ,page_value:null, ,f_page_extend_params:http://tuan.juanpi.com/pintuan/shop/1370792, ,d_page_id:154, ,d_page_value:活动str, ,for_eventid:click_share, ,d_event_id:497, ,event_type_id:0, ,event_id:497, ,event_value:http://wx.juanpi.com/pintuan/item/16389601?utm=106747)
//    // (for_pageid:page_taball, ,page_type_id:10, ,page_level_id:1, ,page_value:最新折扣, ,f_page_extend_params:all, ,
//    // d_page_id:219, ,d_page_value:最新折扣, ,for_eventid:collect_mainpage_loadtime, ,d_event_id:0, ,event_type_id:0, ,event_id:-1, ,event_value:921)
//    val line7 =
//    """{"activityname":"collect_mainpage_loadtime","app_name":"jiu","app_version":"4.1.0","c_label":"C3","c_server":"{\"gid\":\"C3\",\"ugroup\":\"143_223_142_237\"}","cube_position":"","deviceid":"355228562118230","endtime":"1476180047226","endtime_origin":"1476180046863","extend_params":"921","ip":"171.12.94.5","jpid":"ffffffff-f05d-dda3-573b-688300000030","location":"河南省","os":"android","os_version":"5.1","page_extends_param":"all","pagename":"page_tab","pre_extends_param":"","pre_page":"","result":"1","server_jsonstr":"{}","session_id":"1473548965006_jiu_1476180046117","source":"","starttime":"1476180046305","starttime_origin":"1476180045942","ticks":"1473548965006","to_switch":"1","uid":"43721797","utm":"101212"}""".stripMargin
//
//    // base 层
//    val row = Json.parse(line7)
//    val pagename = (row \ "pagename").asOpt[String].getOrElse("").toLowerCase()
//    val page_extends_param = (row \ "page_extends_param").asOpt[String].getOrElse("")
//    // f -> fct
//    val f_page_extend_params = pageAndEventParser.getExtendParams(pagename, page_extends_param)
//
//    val server_jsonstr = (row \ "server_jsonstr").asOpt[String].getOrElse("")
//    val deviceid = (row \ "deviceid").asOpt[String].getOrElse("")
//    val cube_position = (row \ "cube_position").asOpt[String].getOrElse("")
//    val activityname = (row \ "activityname").asOpt[String].getOrElse("").toLowerCase()
//    val extend_params = (row \ "extend_params").asOpt[String].getOrElse("")
//    val app_version = (row \ "app_version").asOpt[String].getOrElse("0")
//    // t -> tmp
//    val t_extend_params = eventParser.getExtendParamsFromBase(activityname, extend_params, app_version)
//
//    // fct 层
//    val cid = pageAndEventParser.getJsonValueByKey(server_jsonstr, "cid")
//
//    if (!cid.isEmpty) {
//      println(cid.toInt)
//    }
//
//    val forEventid = eventParser.getForEventId(cid, activityname, t_extend_params)
//    val eventForPageId = eventParser.getForPageId(cid, f_page_extend_params, pagename)
//    val f_extend_params = eventParser.getForExtendParams(activityname, t_extend_params, cube_position, server_jsonstr)
//    val (pit_type, gsort_key) = pageAndEventParser.getGsortPit(server_jsonstr)
//
//    val d_event_id = 528
//    val d_page_id = 0
//    //    val pPageId = pageAndEventParser.getPageId(d_page_id, f_page_extend_params)
//    val ePageId = pageAndEventParser.getPageId(d_page_id, f_page_extend_params)
//
//    val event_value = eventParser.getEventValue(10, activityname, f_extend_params, server_jsonstr)
//    val event_id = eventParser.getEventId(d_event_id, app_version) + ""
//    //    val event_id = eventParser.getEventId(d_event_id, app_version) + ""
//
//    val forLevelId = if (d_page_id == 254 && f_page_extend_params.nonEmpty) {
//      433
//    } else 0
//
//    val jpid = (row \ "jpid").asOpt[String].getOrElse("")
//    val os = (row \ "os").asOpt[String].getOrElse("")
//    val guId = pageAndEventParser.getGuid(jpid, deviceid, os)
//
//    val flag = eventParser.filterOutlierPageId(activityname, pagename, cid, f_page_extend_params)
//
//    println(guId, deviceid, flag)
//
//    println("pagename:" + pagename, " page_extends_param:" + page_extends_param, " f_page_extend_params:" + f_page_extend_params, " f_extend_params:" + f_extend_params)
//    println("eventParser.getForPageId:" + eventForPageId)
//    println("server_jsonstr:" + server_jsonstr, "deviceid:" + deviceid)
//    println("cid:" + cid, " activityname:" + activityname, " t_extend_params:" + t_extend_params,
//      " cube_position:" + cube_position, " extend_params:" + extend_params, " app_version:" + app_version, " ,forEventid:" + forEventid, " event_id:" + event_id)
//    println(" ePageId:" + ePageId)
//    println("event_value:" + event_value)

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
}
