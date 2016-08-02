package com.juanpi.bi.transformer

import com.alibaba.fastjson.JSON
import com.juanpi.bi.bean.{Event, Page, PageAndEvent, User}
import com.juanpi.bi.hiveUDF.{GetGoodsId, GetMbActionId, GetPageID}
import com.juanpi.bi.sc_utils.DateUtils
import play.api.libs.json.{JsResultException, JsValue, Json}

import scala.collection.mutable

/**
  * Created by gongzi on 2016/7/11.
  */
class MbEventTransformer extends ITransformer {

  def parse(row: JsValue,
            dimpage: mutable.HashMap[String, (Int, Int, String, Int)],
            dimevent: mutable.HashMap[String, (Int, Int)]): (User, PageAndEvent, Page, Event) = {

    // ---------------------------------------------------------------- mb_event ----------------------------------------------------------------
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

    // 用户画像中定义的
    var gid = ""
    var ugroup = ""

    val c_server = (row \ "c_server").asOpt[String].getOrElse("")
    if(!c_server.isEmpty())
    {
      val js_c_server = Json.parse(c_server)
      gid = (js_c_server \ "gid").asOpt[String].getOrElse("0")
      ugroup = (js_c_server \ "ugroup").asOpt[String].getOrElse("0")
    }

    // ------------------------------------------------------------- mb_event -> mb_event_log --------------------------------------------------------------
    val f_page_extend_params = pageAndEventParser.getExtendParams(pagename, page_extends_param)
    val f_pre_extend_params = pageAndEventParser.getExtendParams(pagename, pre_extends_param)
    val t_extend_params = getExtendParamsFromBase(activityname, extend_params, app_version)

    // ---------------------------------------------------------------- mb_event_log -> tmp ----------------------------------------------------------------
    val terminal_id = pageAndEventParser.getTerminalId(os)
    val site_id = pageAndEventParser.getSiteId(app_name)
    val ref_site_id = site_id
    val gu_id = pageAndEventParser.getGuid(jpid, deviceid, os)

    val f_extend_params =
      if ("click_cube_banner".equals(activityname)) {
        if (t_extend_params.contains("ads_id")) {
          val js_t_extend_params = Json.parse(t_extend_params)
          val ads_id = (js_t_extend_params \ "ads_id").asOpt[String]
          "banner" + "::" + ads_id + cube_position
        } else {
          "banner" + "::" + t_extend_params + "::" + cube_position
        }
      }
      else if ("click_cube_block".equals(activityname) && !"{}".equals(server_jsonstr)) {
        server_jsonstr
      }
      else if (server_jsonstr.contains("pit_info")) {
        val js_server_jsonstr = Json.parse(server_jsonstr)
        (js_server_jsonstr \ "pit_info").toString()
      } else if (t_extend_params.contains("pit_info")) {
        val js_t_extend_params = Json.parse(t_extend_params)
        (js_t_extend_params \ "pit_info").toString()
      } else {
        t_extend_params
      }

    // TODO cid = -1
    var cid = ""
    if (server_jsonstr.contains("cid")) {
      val js_server_jsonstr = Json.parse(server_jsonstr)
      cid = (js_server_jsonstr \ "cid").asOpt[String].getOrElse("")
    }

    println("=======>> server_jsonstr::", server_jsonstr)
    val for_pageid = if("-1".equals(cid)) {
      "page_taball"
    } else if("-2".equals(cid)) {
      "page_tabpast_zhe"
    } else if("-3".equals(cid)) {
      "page_tabcrazy_zhe"
    } else if ("-4".equals(cid)) {
      "page_tabjiu"
    } else if ("-5".equals(cid) || "-6".equals(cid)) {
      "page_tabyugao"
    } else if ((!cid.isEmpty && cid.toInt > 0) | (cid == "-100" && (f_page_extend_params == "10045" || f_page_extend_params == "100105"))) {
      "page_tab"
    } else if ((cid == "0") && List("all", "past_zhe", "crazy_zhe", "jiu", "yugao").contains(f_page_extend_params)) {
      ""
    } else if ("page_h5".equals(pagename)) {
      val pid = new GetPageID().evaluate(f_page_extend_params).toInt
      pid match {
        case 34 | 65 | 10069 => "page_active"
        case _ => (pagename + f_page_extend_params).toLowerCase()
      }
    } else if (!"page_tab".equals(pagename)) {
      pagename
    } else {
      (pagename + f_page_extend_params).toLowerCase()
    }

    val for_pre_pageid =
      if ("page_h5".equals(pagename)) {
        var pid = new GetPageID().evaluate(f_pre_extend_params).toInt
        pid match {
          case 34 | 65 | 10069 => "page_active"
          case _ => (pagename + f_pre_extend_params).toLowerCase()
        }
      } else if (!"page_tab".equals(pre_page)) {
        pre_page.toLowerCase()
      }
      else {
        (pre_page + f_pre_extend_params).toLowerCase()
      }

    val for_eventid = if("-6".equals(cid)) {
      "click_yugao_recommendation"
    } else if("-100".equals(cid)) {
      "click_shoppingbag_recommendation"
    } else if("-101".equals(cid)) {
      "click_orderdetails_recommendation"
    } else if ("-102".equals(cid)) {
      "click_detail_recommendation"
    } else if ("click_navigation".equals(activityname)) {
      activityname
    } else {
      (activityname + t_extend_params).toLowerCase()
    }

    val rule_id = getAbinfo(extend_params, "rule_id")
    val test_id = getAbinfo(extend_params, "test_id")
    val select_id = getAbinfo(extend_params, "select")

    val (pit_type, gsort_key) = pageAndEventParser.getGsortPit(server_jsonstr)

    val (sortdate, sorthour, lplid, ptplid) = pageAndEventParser.getGsortKey(gsort_key)

    // --------------------------------------------------------------------> event_reg ------------------------------------------------------------------
    val (d_event_id: Int, event_type_id: Int) = dimevent.get(for_eventid).getOrElse(0, 0)
    val event_id = getEventId(d_event_id, app_version) + ""
    val event_value = getEventValue(event_type_id, activityname, f_extend_params, server_jsonstr)

    val (d_pre_page_id: Int, d_pre_page_type_id: Int, d_pre_page_value: String, d_pre_page_level_id: Int) = dimpage.get(for_pre_pageid).getOrElse(0, 0, "", 0)
    val ref_page_id = pageAndEventParser.getPageId(d_pre_page_id, f_pre_extend_params)
    val ref_page_value = pageAndEventParser.getPageValue(d_pre_page_id, f_pre_extend_params, d_pre_page_type_id, d_pre_page_value)

    val (d_page_id: Int, page_type_id: Int, d_page_value: String, d_page_level_id: Int) = dimpage.get(for_pageid).getOrElse(0, 0, "", 0)
    val page_id = pageAndEventParser.getPageId(d_page_id, f_page_extend_params)
    val page_value = pageAndEventParser.getPageValue(d_page_id, f_page_extend_params, page_type_id, d_page_value)

    val shop_id = pageAndEventParser.getShopId(d_page_id, f_page_extend_params)
    val ref_shop_id = pageAndEventParser.getShopId(ref_page_id, f_pre_extend_params)

    val page_level_id = pageAndEventParser.getPageLevelId(d_page_id, f_extend_params, d_page_level_id)

    val hot_goods_id = if(d_page_id == 250 && !f_page_extend_params.isEmpty && f_page_extend_params.contains("_") && f_page_extend_params.split("_").length > 2)
    {
      new GetGoodsId().evaluate(f_page_extend_params.split("_")(2))
    }
    else {""}

    val page_lvl2_value = pageAndEventParser.getPageLvl2Value(d_page_id, f_page_extend_params, server_jsonstr)

    val ref_page_lvl2_value = pageAndEventParser.getPageLvl2Value(d_pre_page_id, f_pre_extend_params, server_jsonstr)

    // 品宣页点击存储质检类型
    val event_lvl2_value = event_id match {
      case "360" => getJsonValueByKey(server_jsonstr, "item").toString
      case "482"|"481"|"480"|"479"=> getJsonValueByKey(server_jsonstr, "item").toString
      case _ => ""
    }

    val jpk = 0
    val loadTime = getJsonValueByKey(server_jsonstr, "_t").toString

    println("======>> page_id :: " + page_id)
    val (date, hour) = DateUtils.dateHourStr(endtime.toLong)
    val table_source = "mb_event"

    val user = User.apply(gu_id, uid, utm, "", session_id, terminal_id, app_version, site_id, ref_site_id, ctag, location, jpk, ugroup, date, hour)
    val pe = PageAndEvent.apply(page_id, page_value, ref_page_id, ref_page_value, shop_id, ref_shop_id, page_level_id, starttime, endtime, hot_goods_id, page_lvl2_value, ref_page_lvl2_value, pit_type, sortdate, sorthour, lplid, ptplid, gid, table_source)
    val page = Page.apply(source, ip, "", "", deviceid, to_switch)
    val event = Event.apply(event_id, event_value, event_lvl2_value, rule_id, test_id, select_id, loadTime)
    (user, pe, page, event)
  }

  def getJsonValueByKey(jsonStr: String, key: String): Unit = {
    val js = Json.parse(jsonStr)
    (js \ key).asOpt[String].getOrElse("")
  }

  def getEventId(d_event_id: Int, app_version: String): Int = {
    val app_ver = getVersionNum(app_version)
    d_event_id match {
      case 0 => -1
      case b if (app_ver > 323 && d_event_id != 279) => d_event_id
      case _ => -999
    }
  }

    def getEventValue(event_type_id: Int, activityname: String, extend_params: String, server_jsonstr: String): String =
    {
      //  -- gognzi && lielie,过滤掉商品流坑位数据中非当天的数据
      val js = Json.parse(server_jsonstr)
      var oper_time = ""
      if (server_jsonstr.contains("_t")) {
        oper_time = (js \ "_t").asOpt[String].getOrElse("")
      }
      // TODO
      if (event_type_id == 10) {
        if (activityname.contains("click_cube")) {
          extend_params
        } else if (!server_jsonstr.contains("_t") || oper_time.isEmpty) {
          ""
        } else {
          extend_params
        }
      } else extend_params
    }

    def getAbinfo(extend_params: String, arg: String): String = {
      if (extend_params.contains(arg)) {
        var ab_info = (Json.parse(extend_params) \ "ab_info").toString()
        (Json.parse(ab_info) \ arg).toString()
      }
      else ""
    }

    def getExtendParamsFromBase(activityname: String, extend_params: String, app_version: String): String = {
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
        case "click_goods_share" => new GetGoodsId().evaluate(extend_params)
        case a if "click_temai_inpage_joinbag" == activityname.toLowerCase() && getVersionNum(app_version) < app_version323 => new GetGoodsId().evaluate(extend_params)
        case _ => extend_params.toLowerCase()
      }
    }

    /**
      *
      * @param activityname
      * @return
      */
    def getActivityid(activityname: String): Int = {
      new GetMbActionId().evaluate(activityname.toLowerCase())
    }

    def getVersionNum(app_version: String): Int = {
      // TODO
      app_version.replace(".", "").toInt
    }

    // 返回解析的结果
    def transform(line: String, dimpage: mutable.HashMap[String, (Int, Int, String, Int)], dimevent: mutable.HashMap[String, (Int, Int)]): (String, String, Any) = {
      //play
      val row = Json.parse(line)
      val ticks = (row \ "ticks").asOpt[String].getOrElse("")

      // TODO 逻辑待优化
      if (ticks.length() >= 13) {
        // 解析逻辑
        var gu_id = ""
        try {
          gu_id = pageAndEventParser.getGuid((row \ "jpid").asOpt[String].getOrElse(""),
            (row \ "deviceid").asOpt[String].getOrElse(""),
            (row \ "os").asOpt[String].getOrElse("")
          )
        } catch {
          //使用模式匹配来处理异常
          case ex: IllegalArgumentException => println(ex.getMessage())
          case ex: RuntimeException => {
            println(ex.getMessage())
          }
          case ex: JsResultException => println(ex.getStackTraceString, "\n======>>异常数据:" + row)
          case ex: Exception => println(ex.getStackTraceString, "\n======>>异常数据:" + row)
        }

        if(!gu_id.isEmpty) {
          val res = parse(row, dimpage, dimevent)
          (DateUtils.dateGuidPartitions((row \ "endtime").as[String].toLong, gu_id).toString, "event", res)
        } else {
          ("", "", None)
        }
      }
      else {
        ("", "", None)
      }
  }

}

// for test
object MbEventTransformer{
  def main(args: Array[String]) {
//    val event = """{"session_id":"1453286581908_jiu_1457423937672","ticks":"1453286581908","uid":"16739625","utm":"101225","app_name":"jiu","app_version":"3.3.8","os":"android","os_version":"5.1.1","deviceid":"0","jpid":"00000000-3be0-c4d6-b09c-156062841d62","to_switch":"1","location":"河北省","c_label":"C2","activityname":"click_cube_goods","extend_params":{"pit_info":"goods::5208686::1_22","ab_info":{"rule_id":"","test_id":"","select":""}},"source":"","cube_position":"1_22","server_jsonstr":{},"starttime":"1457425815507","endtime":"1457425815507","result":"1","pagename":"page_home_brand_in","page_extends_param":"1620540_1345584_5608626","pre_page":"page_temai_goods","pre_extends_param":"5508676","gj_page_names":"page_home_brand_in,page_home_brand_in,page_tab,page_home_brand_in","gj_ext_params":"1435453_1445587_5139662,1435453_1445587_5139662,all,1620540_1345584_5608626","starttime_origin":"1457425815189","endtime_origin":"1457425815189","ip":"106.8.147.163"}"""
//    val b = JSON.parseObject(event)
//    println(b.get("session_id"))
    val gsort_key = "POSTION_SORT_65_20160525_12_63"
    val sortdate = Array(gsort_key.split("_")(3).substring(0, 4), gsort_key.split("_")(3).substring(4, 6), gsort_key.split("_")(3).substring(6, 8)).mkString("-")
    val sorthour = gsort_key.split("_")(4)
    val lplid = gsort_key.split("_")(5)
    var ptplid = ""
    if(gsort_key.split("_").length > 6 ){
      ptplid = gsort_key.split("_")(6)
    }
    println(sortdate, sorthour, lplid, ptplid)

  }
}