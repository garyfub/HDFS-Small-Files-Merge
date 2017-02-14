package com.juanpi.bi.transformer

import com.juanpi.bi.bean.{Event, Page, PageAndEvent, User}
import com.juanpi.bi.sc_utils.DateUtils
import com.juanpi.hive.udf.{GetGoodsId, GetMbActionId}
import play.api.libs.json.{JsValue, Json}

import scala.collection.mutable

/**
  * Created by gongzi on 2016/7/11.
  */
class MbEventTransformer {

  /**
    *
    * @param line
    * @param dimpage
    * @param dimevent
    * @return
    */
  def logParser(line: String,
                dimpage: mutable.HashMap[String, (Int, Int, String, Int)],
                dimevent: mutable.HashMap[String, (Int, Int, Int)],
                fCate: mutable.HashMap[String, String]): (String, String, Any) = {

    val row = Json.parse(line)
    val ticks = (row \ "ticks").asOpt[String].getOrElse("")
    val jpid = (row \ "jpid").asOpt[String].getOrElse("")
    val deviceId = (row \ "deviceid").asOpt[String].getOrElse("")
    val os = (row \ "os").asOpt[String].getOrElse("")

    val starttime_origin = (row \ "starttime_origin").asOpt[String].getOrElse("")

    if(starttime_origin.isEmpty) {
      return ("", "", null)
    }

    val originDateStr = DateUtils.dateStr(starttime_origin.toLong)

    val sDate = DateUtils.getWeekAgoDateStr()
    val eDate = DateUtils.getWeekLaterDateStr()

    val startTime = if(originDateStr > sDate && originDateStr < eDate) {
      starttime_origin
    } else {
      ""
    }

    if(startTime.isEmpty) {
      return ("", "", null)
    }

    val partitionTime = startTime

    // TODO 逻辑待优化
    if (ticks.length() >= 13) {
      var gu_id = ""
      try {
        gu_id = pageAndEventParser.getGuid(jpid, deviceId, os)
      } catch {
        case ex: Exception => { println("=========>>pageAndEventParser.getGuid: " + ex.getStackTraceString) }
        println("=======>> Event: getGuid Exception 0000 ======>>异常数据:" + row)
      }

      val ret = if (gu_id.nonEmpty && !gu_id.equalsIgnoreCase("null")) {
        val server_jsonstr = (row \ "server_jsonstr").asOpt[String].getOrElse("")
        val loadTime = pageAndEventParser.getJsonValueByKey(server_jsonstr, "_t")

        // 如果loadTime非空，就需要判断是否是当天的数据，如果不是，需要过滤掉,因此不需要处理
        if (loadTime.nonEmpty &&
          DateUtils.dateStr(partitionTime.toLong) != DateUtils.dateStr(loadTime.toLong * 1000)) {
          ("", "", None)
        }
        else {
          try {
            val res = parse(partitionTime, row, dimpage, dimevent, fCate)
            if (res == null) {
              ("", "", None)
            }
            else {
              val (user: User, pageAndEvent: PageAndEvent, page: Page, event: Event) = res

              val res_str = pageAndEventParser.combineTuple(user, pageAndEvent, page, event).map(x => x match {
                case y if y == null || y.toString.isEmpty => "\\N"
                case _ => x
              }).mkString("\001")

              val partitionStr = DateUtils.dateGuidPartitions(partitionTime.toLong, gu_id)
              (partitionStr, "event", res_str)
            }
          }
          catch {
            //使用模式匹配来处理异常
            case ex:Exception => {
              println("=======>> parse Exception: " + ex.getStackTraceString)
            }
              println("=======>> Event: parse Exception!!" + "======>>异常数据:" + row)
            ("", "", None)
          }
        }
      } else {
        println("=======>> Event: GU_ID IS NULL 1111!! ======>>异常数据:" + row)
        ("", "", None)
      }
      ret
    } else {
      println("=======>> Event: ROW IS NULL 22222 ======>>异常数据:" + row)
      ("", "", None)
    }
  }

  def parse(partitionTime: String,
            row: JsValue,
            dimPage: mutable.HashMap[String, (Int, Int, String, Int)],
            dimEvent: mutable.HashMap[String, (Int, Int, Int)],
            fCate: mutable.HashMap[String, String]): (User, PageAndEvent, Page, Event) = {

    // ---------------------------------------------------------------- mb_event ----------------------------------------------------------------
    val session_id = (row \ "session_id").asOpt[String].getOrElse("")
    val activityname = (row \ "activityname").asOpt[String].getOrElse("").toLowerCase()

    val starttime_origin = (row \ "starttime_origin").asOpt[String].getOrElse("")
    val endtime_origin = (row \ "endtime_origin").asOpt[String].getOrElse("")

    val endTime = if (endtime_origin.isEmpty) {
      starttime_origin
    } else {
      endtime_origin
    }

    val uid = (row \ "uid").asOpt[String].getOrElse("0")
    val extend_params = (row \ "extend_params").asOpt[String].getOrElse("")
    // utm 的值还会改变，故定义成var
    val utm = (row \ "utm").asOpt[String].getOrElse("")
    val source = ""

    val app_name = (row \ "app_name").asOpt[String].getOrElse("")
    val app_version = (row \ "app_version").asOpt[String].getOrElse("0")
    val os = (row \ "os").asOpt[String].getOrElse("")
    val pagename = (row \ "pagename").asOpt[String].getOrElse("").toLowerCase()
    val page_extends_param = (row \ "page_extends_param").asOpt[String].getOrElse("")
    val deviceid = (row \ "deviceid").asOpt[String].getOrElse("")
    val pre_page = (row \ "pre_page").asOpt[String].getOrElse("")
    val pre_extends_param = (row \ "pre_extends_param").asOpt[String].getOrElse("")
    val jpid = (row \ "jpid").asOpt[String].getOrElse("")
    val ip = ""
    val to_switch = (row \ "to_switch").asOpt[String].getOrElse("")
    val cube_position = (row \ "cube_position").asOpt[String].getOrElse("")
    val location = (row \ "location").asOpt[String].getOrElse("")
    val ctag = (row \ "c_label").asOpt[String].getOrElse("")
    val server_jsonstr = (row \ "server_jsonstr").asOpt[String].getOrElse("")

    val loadTime = pageAndEventParser.getJsonValueByKey(server_jsonstr, "_t")
    val ug_id = pageAndEventParser.getJsonValueByKey(server_jsonstr, "_z")

    // 用户画像中定义的
    val c_server = (row \ "c_server").asOpt[String].getOrElse("")
    val (gid, ugroup) = if (c_server.nonEmpty) {
      val js_c_server = Json.parse(c_server)
      val gid = (js_c_server \ "gid").asOpt[String].getOrElse("0")
      val ugroup = (js_c_server \ "ugroup").asOpt[String].getOrElse("0")
      (gid, ugroup)
    } else {
      ("0", "0")
    }

    // ---------------------------------------------------------------- mb_event_log -> tmp ----------------------------------------------------------------
    val terminal_id = pageAndEventParser.getTerminalId(os)
    val site_id = pageAndEventParser.getSiteId(app_name)
    val ref_site_id = site_id
    val gu_id = pageAndEventParser.getGuid(jpid, deviceid, os)

    // ------------------------------------------------------------- mb_event -> mb_event_log --------------------------------------------------------------
    val f_page_extend_params = pageAndEventParser.getExtendParams(pagename, page_extends_param)
    val f_pre_extend_params = pageAndEventParser.getExtendParams(pagename, pre_extends_param)

    val t_extend_params = eventParser.getExtendParamsFromBase(activityname, extend_params, app_version)
    val f_extend_params = eventParser.getForExtendParams(activityname, t_extend_params, cube_position, server_jsonstr)

    val cid = pageAndEventParser.getJsonValueByKey(server_jsonstr, "cid")

    val flag = eventParser.filterOutlierPageId(activityname, pagename, cid, f_page_extend_params)

    if (flag) {
      // 如果需要过滤，本条数据解析致此结束
      return null
    }

    val forPageId = eventParser.getForPageId(cid, f_page_extend_params, pagename)

    val forPrePageId = eventParser.getForPrePageId(pagename, f_pre_extend_params, pre_page)

    val forEventId = eventParser.getForEventId(cid, activityname, t_extend_params)

    val rule_id = ""
    val (test_id,select_id) = eventParser.getAbinfo(server_jsonstr)

    val (pit_type, gsort_key) = eventParser.getGsortPit(server_jsonstr)

    val (sortdate, sorthour, lplid, ptplid) = eventParser.getGsortKey(gsort_key)

    // --------------------------------------------------------------------> event_reg ------------------------------------------------------------------
    val (d_event_id: Int, event_type_id: Int, event_level_id: Int) = dimEvent.get(forEventId).getOrElse(0, 0, 0)
    val event_id = eventParser.getEventId(d_event_id, app_version)
    val event_value = eventParser.getEventValue(event_type_id, activityname, f_extend_params, server_jsonstr)

    // d_ 表示数据字段值从dim表出的
    val (d_pre_page_id: Int, d_pre_page_type_id: Int, d_pre_page_value: String, d_pre_page_level_id: Int) = dimPage.get(forPrePageId).getOrElse(0, 0, "", 0)
    val ref_page_id = pageAndEventParser.getPageId(d_pre_page_id, f_pre_extend_params)
    val ref_page_value = eventParser.getPageValue(d_pre_page_id, f_pre_extend_params, cid, d_pre_page_type_id, d_pre_page_value)

    val (d_page_id: Int, page_type_id: Int, d_page_value: String, d_page_level_id: Int) = dimPage.get(forPageId).getOrElse(0, 0, "", 0)
    val page_id = pageAndEventParser.getPageId(d_page_id, f_page_extend_params)

    val forLevelId = if (d_page_id == 254 && f_page_extend_params.nonEmpty) {
      fCate.get(f_page_extend_params).getOrElse("0")
    } else "0"

    val page_value = eventParser.getPageValue(d_page_id, f_page_extend_params, cid, page_type_id, d_page_value)

    val shop_id = pageAndEventParser.getShopId(d_page_id, f_page_extend_params)
    val ref_shop_id = pageAndEventParser.getShopId(ref_page_id, f_pre_extend_params)

    val page_level_id = pageAndEventParser.getEventPageLevelId(event_level_id, d_page_id, f_page_extend_params, d_page_level_id, forLevelId)

    val hot_goods_id = if (d_page_id == 250 && !f_page_extend_params.isEmpty && f_page_extend_params.contains("_") && f_page_extend_params.split("_").length > 2) {
      new GetGoodsId().evaluate(f_page_extend_params.split("_")(2))
    }
    else {
      ""
    }

    val page_lvl2_value = eventParser.getPageLvl2Value(d_page_id, f_page_extend_params)

    val ref_page_lvl2_value = eventParser.getPageLvl2Value(d_pre_page_id, f_pre_extend_params)

    // 品宣页点击存储质检类型
    val event_lvl2_value = event_id.toString match {
      case "360" => pageAndEventParser.getJsonValueByKey(server_jsonstr, "item")
      case "482" | "481" | "480" | "479" => pageAndEventParser.getJsonValueByKey(server_jsonstr, "_rmd")
      case _ => ""
    }

    val jpk = 0

    val (date, hour) = DateUtils.dateHourStr(partitionTime.toLong)

    val table_source = "mb_event"

    val user = User.apply(gu_id, uid, utm, "", session_id, terminal_id, app_version, site_id, ref_site_id, ctag, location, jpk, ugroup, date, hour)
    val pe = PageAndEvent.apply(page_id, page_value, ref_page_id, ref_page_value, shop_id, ref_shop_id, page_level_id, starttime_origin, endTime, hot_goods_id, page_lvl2_value, ref_page_lvl2_value, pit_type, sortdate, sorthour, lplid, ptplid, gid, table_source)
    val page = Page.apply(source, ip, "", "", deviceid, to_switch)
    val event = Event.apply(event_id.toString, event_value, event_lvl2_value, rule_id, test_id, select_id, loadTime, ug_id)

    // TODO 测试代码，测试后需要删掉
    if (-1 == page_id) {
      println("for_pageid:" + forPageId,
        " ,page_type_id:" + page_type_id,
        " ,page_level_id:" + page_level_id,
        " ,cid" + cid,
        " ,f_page_extend_params:" + f_page_extend_params,
        " ,pagename:" + pagename,
        " ,page_value:" + page_value,
        " ,f_page_extend_params:" + f_page_extend_params,
        " ,d_page_id:" + d_page_id,
        " ,d_page_value:" + d_page_value,
        " ,for_eventid:" + forEventId,
        " ,event_id:" + event_id,
        " ,d_event_id:" + d_event_id,
        " ,event_type_id:" + event_type_id,
        " ,cid:" + cid,
        " ,activityname:" + activityname,
        " ,t_extend_params:" + t_extend_params,
        " ,event_value:" + event_value)
      println("page_id=-1 ==> 原始数据为：" + row)
    }

    if (-1 == event_id && "click_temai_inpage_joinbag".equals(activityname)) {
      println("for_pageid:" + forPageId,
        "page_id:" + page_id,
        " ,page_type_id:" + page_type_id,
        " ,page_level_id:" + page_level_id,
        " ,page_value:" + page_value,
        " ,f_page_extend_params:" + f_page_extend_params,
        " ,d_page_id:" + d_page_id,
        " ,d_page_value:" + d_page_value,
        " ,for_eventid:" + forEventId,
        " ,event_id:" + event_id,
        " ,d_event_id:" + d_event_id,
        " ,event_type_id:" + event_type_id,
        " ,cid:" + cid,
        " ,activityname:" + activityname,
        " ,t_extend_params:" + t_extend_params,
        " ,event_value:" + event_value)
      println("event_id=-1 ==> 原始数据为：" + row)
    }

    (user, pe, page, event)
  }

  /**
    * @param activityname
    * @return
    */
  def getActivityid(activityname: String): Int = {
    new GetMbActionId().evaluate(activityname)
  }
}