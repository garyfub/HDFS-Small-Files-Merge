package com.juanpi.bi.transformer

import com.alibaba.fastjson.JSON
import com.juanpi.bi.bean.{Event, Page, PageAndEvent, User}
import com.juanpi.bi.hiveUDF.{GetGoodsId, GetMbActionId, GetPageID}
import com.juanpi.bi.init.InitConfig._
import com.juanpi.bi.sc_utils.DateUtils
import com.juanpi.bi.streaming.DateHour
import org.apache.hadoop.hbase.client.{Get, Put}
import play.api.libs.json.{JsValue, Json}

import scala.collection.mutable

/**
  * Created by gongzi on 2016/7/11.
  */
class MbEventTransformer extends ITransformer {

  def parse(row: JsValue,
            dimpage: mutable.HashMap[String, (Int, Int, String, Int)],
            dimevent: mutable.HashMap[String, Int]): (User, PageAndEvent, Page, Event) = {
    // mb_event
    val ticks = (row \ "ticks").asOpt[String].getOrElse("")
    val session_id = (row \ "session_id").asOpt[String].getOrElse("")
    val activityname = (row \ "activityname").asOpt[String].getOrElse("").toLowerCase()
    val starttime = (row \ "starttime").asOpt[String].getOrElse("")
    val endtime = (row \ "endtime").asOpt[String].getOrElse("")
    val result = (row \ "result").asOpt[String].getOrElse("")
    val uid = (row \ "uid").asOpt[String].getOrElse("")
    val extend_params = (row \ "extend_params").asOpt[String].getOrElse("")
    // utm 的值还会改变，故定义成var
    var utm = (row \ "utm").asOpt[String].getOrElse("")
    val source = ""
    val starttime_origin = (row \ "starttime_origin").asOpt[String].getOrElse("")
    val endtime_origin = (row \ "endtime_origin").asOpt[String].getOrElse("")
    val app_name = (row \ "app_name").asOpt[String].getOrElse("")
    val app_version = (row \ "app_version").asOpt[String].getOrElse("")
    val os = (row \ "os").asOpt[String].getOrElse("")
    val pagename = (row \ "pagename").asOpt[String].getOrElse("").toLowerCase()
    val page_extends_param = (row \ "page_extends_param").asOpt[String].getOrElse("")
    val deviceid = (row \ "deviceid").asOpt[String].getOrElse("")
    val pre_page = (row \ "pre_page").asOpt[String].getOrElse("")
    // 字段与pageinfo中的不太一样
    val pre_extends_param = (row \ "pre_extends_param").asOpt[String].getOrElse("")
    val gj_page_names = (row \ "gj_page_names").asOpt[String].getOrElse("")
    val gj_ext_params = (row \ "gj_ext_params").asOpt[String].getOrElse("")
    val jpid = (row \ "jpid").asOpt[String].getOrElse("")
    val ip = ""
    val to_switch = (row \ "to_switch").asOpt[String].getOrElse("")
    val cube_position = (row \ "cube_position").asOpt[String].getOrElse("")
    val location = (row \ "location").asOpt[String].getOrElse("")
    val ctag = (row \ "c_label").asOpt[String].getOrElse("")
    val server_jsonstr = (row \ "server_jsonstr").asOpt[String].getOrElse("")

    var gid = 0
    var ugroup = 0
    val c_server = (row \ "c_server").asOpt[String].getOrElse("")
    if (!c_server.isEmpty()) {
      val js_c_server = Json.parse(c_server)
      gid = (js_c_server \ "gid").asOpt[Int].getOrElse(0)
      ugroup = (js_c_server \ "ugroup").asOpt[Int].getOrElse(0)
    }

    val gu_id = pageAndEventParser.getGuid(jpid, deviceid, os)
    val terminal_id = pageAndEventParser.getTerminalId(os)
    val site_id = pageAndEventParser.getSiteId(app_name)
    val ref_site_id = site_id

    var final_extend_params =
      if ("click_cube_banner".equals(activityname)) {
        if (extend_params.contains("ads_id")) {
          var js_extend_params = Json.parse(extend_params)
          var ads_id = (js_extend_params \ "ads_id").asOpt[String]
          "banner" + "::" + ads_id + cube_position
        } else {
          "banner" + "::" + extend_params + "::" + cube_position
        }
      }
      else if ("click_cube_block".equals(activityname) && !"{}".equals(server_jsonstr)) {
        server_jsonstr
      }
      else if (server_jsonstr.contains("pit_info")) {
        var js_server_jsonstr = Json.parse(server_jsonstr)
        (js_server_jsonstr \ "pit_info")
      } else if (extend_params.contains("pit_info")) {
        var js_extend_params = Json.parse(extend_params)
        (js_extend_params \ "pit_info")
      } else {
        extend_params
      }

    val for_eventid =
      if (server_jsonstr.contains("cid")) {
        var js_server_jsonstr = Json.parse(server_jsonstr)
        var cid = (js_server_jsonstr \ "cid").toString()
        cid match {
          case "-6" => "click_yugao_recommendation"
          case "-100" => "click_shoppingbag_recommendation"
          case "-101" => "click_orderdetails_recommendation"
          case "-102" => "click_detail_recommendation"
        }
      }
      else if ("click_navigation".equals(activityname)) {
        activityname
      } else {
        (activityname + extend_params).toLowerCase()
      }

    // 扩展参数
    val extend_params_1 = pageAndEventParser.getExtendParams(pagename, page_extends_param)
    val pre_extend_params_1 = pageAndEventParser.getExtendParams(pagename, pre_extends_param)

    val for_pageid =
      if (server_jsonstr.contains("cid"))
      {
        var js_server_jsonstr = Json.parse(server_jsonstr)
        var cid = (js_server_jsonstr \ "cid").toString()
        cid match {
          case "-1" => "page_taball"
          case "-2" => "page_tabpast_zhe"
          case "-3" => "page_tabcrazy_zhe"
          case "-4" => "page_tabjiu"
          case "-5" | "-6" => "page_tabyugao"
          case _cid if cid.toInt > 0 | (cid == "-100" && (page_extends_param == "10045" || page_extends_param == "100105")) => "page_tab"
          case _cid if (cid == "0") && List("all", "past_zhe", "crazy_zhe", "jiu", "yugao").contains(page_extends_param) => ""
        }
      } else if ("page_h5".equals(pagename))
      {
        val pid = new GetPageID().evaluate(page_extends_param).toInt
        pid match {
          case 34 | 65 | 10069 => "page_active"
          case _ => (pagename + page_extends_param).toLowerCase()
        }
      } else if (!"page_tab".equals(pagename))
      {
        pagename
      } else
      {
        (pagename + page_extends_param).toLowerCase()
      }

    val for_pre_pageid =
      if ("page_h5".equals(pagename))
      {
        var pid = new GetPageID().evaluate(pre_extends_param).toInt
        pid match {
          case 34 | 65 | 10069 => "page_active"
          case _ => (pagename + pre_extends_param).toLowerCase()
        }
      } else if(!"page_tab".equals(pre_page))
      {
        pre_page.toLowerCase()
      }
      else
      {
        (pre_page + pre_extends_param).toLowerCase()
      }

    val rule_id = getAbinfo(extend_params, "rule_id")
    val test_id = getAbinfo(extend_params, "test_id")
    val select_id = getAbinfo(extend_params, "select")

    var pit_type = 0
    var gsort_key = ""
    if(!server_jsonstr.isEmpty())
    {
      val js_server_jsonstr = Json.parse(server_jsonstr)
      pit_type = (js_server_jsonstr \ "_pit_type").asOpt[Int].getOrElse(0)
      gsort_key = (js_server_jsonstr \ "_gsort_key").asOpt[String].getOrElse("")
    }

    val (sortdate, sorthour, lplid, ptplid) = if(!gsort_key.isEmpty) {
      val sortdate = Array(gsort_key.split("_")(3).substring(0, 4),gsort_key.split("_")(3).substring(4, 6),gsort_key.split("_")(3).substring(6, 8)).mkString("-")
      val sorthour = gsort_key.split("_")(4)
      val lplid = gsort_key.split("_")(6)
      val ptplid = gsort_key.split("_")(6)
      (sortdate, sorthour, lplid, ptplid)
    }
    else ("", "", "", "")

    val (d_page_id: Int, page_type_id: Int, d_page_value: String, d_page_level_id: Int) = dimpage.get(for_pageid).getOrElse(0, 0, "", 0)
    val event_id =
    val event_type_id = 0
    val event_value = getEventValue(event_type_id, activityname, extend_params, server_jsonstr)
    val gu_create_time = ""
    val user = User.apply(gu_id, utm, gu_create_time, session_id, terminal_id, app_version, site_id, ref_site_id, ctag)
    val pe = PageAndEvent.apply(page_id, page_value, ref_page_id, ref_page_value, page_level_id, starttime, endtime, hot_goods_id, page_lvl2_value, ref_page_lvl2_value, jpk, pit_type, sortdate, sorthour, lplid, ptplid, gid, ugroup, table_source)
    val page = Page.apply("", "", "", "", "", "")
    val event = Event.apply(event_id,event_value,event_lvl2_value,rule_id,test_id,select_id)
    (user, pe, page, event)

  }


  def getEventId(): String = {
    CASE
    WHEN b.event_id IS NULL THEN -1
    WHEN a.app_version >= '3.2.3'
    OR        b.event_id <> 279 THEN b.event_id
    ELSE - 999
    end event_id,
""
  }

  def getEventValue(event_type_id: Int, activityname: String, extend_params: String, server_jsonstr: String): String =
  {
//    -- gognzi && lielie,过滤掉商品流坑位数据中非当天的数据
    val js = Json.parse(server_jsonstr)
    var oper_time = ""
    if(server_jsonstr.contains("_t")) {
      oper_time = (js \ "_t").asOpt[String].getOrElse("")
    }

    // TODO
    if(event_type_id == 10){
      if(activityname.contains("click_cube")) {
        extend_params
      } else if(!server_jsonstr.contains("_t") || oper_time.isEmpty) {
        ""
    } else {
      extend_params
    }
  }


  def getAbinfo(extend_params: String, arg: String): String =
  {
    if(extend_params.contains(arg))
    {
      var ab_info = (Json.parse(extend_params) \ "ab_info").toString()
      (Json.parse(ab_info) \ arg).toString()
    }
    else ""
  }

  def getExtendParamsFromBase(activityname: String, extend_params: String, app_version: String): String =
  {
    // 老版本 3.2.3
    val app_version323 = 323
    activityname match {
    case "click_temai_inpage_qq" => new GetGoodsId().evaluate(extend_params)
    case "click_temai_returngoods" => new GetGoodsId().evaluate(extend_params)
    case "click_temai_inpage_share" => new GetGoodsId().evaluate(extend_params)
    case "click_temai_inpage_collect" => new GetGoodsId().evaluate(extend_params)
    case "click_temai_inpage_cancelcollect" => new GetGoodsId().evaluate(extend_params)
    case "click_temai_orderdetails_complex" => new GetGoodsId().evaluate(extend_params)
    case "click_goods_tb" => new GetGoodsId().evaluate(extend_params)
    case "click_goods_cancel" => new GetGoodsId().evaluate(extend_params)
    case "click_goods_collection" => new GetGoodsId().evaluate(extend_params)
    case "click_goods_shar" => new GetGoodsId().evaluate(extend_params)
    case a if "click_temai_inpage_joinbag" == activityname.toLowerCase() && getVersionNum(app_version) < app_version323 =>  new GetGoodsId().evaluate(extend_params)
    case _ => extend_params.toLowerCase()
    }
  }

  /**
    *
    * @param activityname
    * @return
    */
  def getActivityid(activityname: String): Int =
  {
    new GetMbActionId().evaluate(activityname.toLowerCase())
  }

  def getVersionNum(app_version: String): Int =
  {
    "3.2.3".replace(".", "").toInt
  }

  // 返回解析的结果
  def transform(line: String): (String, String) = {

    // fastjson 也可以用。
    // val row = JSON.parseObject(line)

    //play
    val row = Json.parse(line)
    val ticks = (row \ "ticks").asOpt[String].getOrElse("")

    // TODO 逻辑待优化
    if(ticks.length() >= 13)
    {
      // 解析逻辑
      val res = parse(row)

      if (row != null) {
        (DateUtils.dateHour((row \ "endtime").as[String].toLong).toString, res.toString())
      } else {
        (DateHour("1970-01-01", "1").toString, line)
      }
    }
    else null
  }
}

// for test
object MbEventTransformer{
  def main(args: Array[String]) {
    val event = """{"session_id":"1453286581908_jiu_1457423937672","ticks":"1453286581908","uid":"16739625","utm":"101225","app_name":"jiu","app_version":"3.3.8","os":"android","os_version":"5.1.1","deviceid":"0","jpid":"00000000-3be0-c4d6-b09c-156062841d62","to_switch":"1","location":"河北省","c_label":"C2","activityname":"click_cube_goods","extend_params":{"pit_info":"goods::5208686::1_22","ab_info":{"rule_id":"","test_id":"","select":""}},"source":"","cube_position":"1_22","server_jsonstr":{},"starttime":"1457425815507","endtime":"1457425815507","result":"1","pagename":"page_home_brand_in","page_extends_param":"1620540_1345584_5608626","pre_page":"page_temai_goods","pre_extends_param":"5508676","gj_page_names":"page_home_brand_in,page_home_brand_in,page_tab,page_home_brand_in","gj_ext_params":"1435453_1445587_5139662,1435453_1445587_5139662,all,1620540_1345584_5608626","starttime_origin":"1457425815189","endtime_origin":"1457425815189","ip":"106.8.147.163"}"""
    val b = JSON.parseObject(event)
    println(b.get("session_id"))

    val me = new MbEventTransformer()
    println(me.getVersionNum(""))

    val row = Json.parse(event)
    println(row)
  }
}