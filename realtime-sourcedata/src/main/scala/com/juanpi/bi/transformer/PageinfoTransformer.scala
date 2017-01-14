package com.juanpi.bi.transformer

import com.juanpi.bi.bean.{Event, Page, PageAndEvent, User}
import com.juanpi.bi.sc_utils.DateUtils
import com.juanpi.hive.udf.GetGoodsId
import play.api.libs.json.{JsValue, Json}

import scala.collection.mutable
/**
  * 解析逻辑的具体实现
  */
class PageinfoTransformer {

  // 返回解析的结果
  def logParser(line: String,
                dimPage: mutable.HashMap[String, (Int, Int, String, Int)],
                dimEvent: mutable.HashMap[String, (Int, Int, Int)],
                fCate: mutable.HashMap[String, String]): (String, String, Any) = {

    val row = Json.parse(line.replaceAll("null", """\\"\\""""))

    if (row != null) {
      var gu_id = ""
      val ticks = (row \ "ticks").asOpt[String].getOrElse("")
      val jpid = (row \ "jpid").asOpt[String].getOrElse("")
      val deviceId = (row \ "deviceid").asOpt[String].getOrElse("")
      val os = (row \ "os").asOpt[String].getOrElse("")

      val starttime_origin = (row \ "starttime_origin").asOpt[String].getOrElse("")

      val originDateStr = DateUtils.dateStr(starttime_origin.toLong)

      val sDate = DateUtils.getWeekAgoDateStr()
      val eDate = DateUtils.getWeekLaterDateStr()

      if(starttime_origin.isEmpty) {
        return ("", "", null)
      }

      val startTime = if(originDateStr > sDate && originDateStr < eDate) {
        starttime_origin
      } else {
        ""
      }

      if(startTime.isEmpty) {
        return ("", "", null)
      }

      val partitionTime = startTime

      try
      {
        gu_id = pageAndEventParser.getGuid(jpid, deviceId, os)
      } catch {
        case ex:Exception => { println(ex.getStackTraceString)}
          println("=======>> Event: getGuid Exception 0000 ======>>异常数据:" + row)
      }

      val ret = if(gu_id.nonEmpty) {
        try {
          val res = parse(partitionTime, row, dimPage, fCate)
          val partitionStr = DateUtils.dateGuidPartitions(partitionTime.toLong, gu_id)
          (partitionStr, "page", res)
        } catch {
          case ex:Exception => {
            println("=======>> parse Exception" + ex.getStackTraceString)
            ex.printStackTrace()
          }
            println("=======>> Page-real: parse Exception!!" + "======>>异常数据:" + row)
            ("", "", None)
        }
      } else {
        println("=======>> Page: GU_ID IS NULL 1111 =====>>异常数据:" + row)
        ("", "", None)
      }
      ret
    } else {
      println("=======>> Page: ROW IS NULL 2222 =====>>异常数据:" + row)
      ("", "", None)
    }
  }

  private def parse(partitionTime: String,
                    row: JsValue,
                    dimPage: mutable.HashMap[String, (Int, Int, String, Int)],
                    fCate: mutable.HashMap[String, String]): (User, PageAndEvent, Page, Event) = {

    val session_id = (row \ "session_id").asOpt[String].getOrElse("")
    val pageName = (row \ "pagename").asOpt[String].getOrElse("").toLowerCase()
    val prePage = (row \ "pre_page").asOpt[String].getOrElse("")
    val uid = (row \ "uid").asOpt[String].getOrElse("0")
    val extendParams = (row \ "extend_params").asOpt[String].getOrElse("")
    val appName = (row \ "app_name").asOpt[String].getOrElse("")
    val appVersion = (row \ "app_version").asOpt[String].getOrElse("")
    val os = (row \ "os").asOpt[String].getOrElse("")
    val utm = (row \ "utm").asOpt[String].getOrElse("0")
    val source = (row \ "source").asOpt[String].getOrElse("")
    val starttime_origin = (row \ "starttime_origin").asOpt[String].getOrElse("")
    val endtime_origin = (row \ "endtime_origin").asOpt[String].getOrElse("")

    val endTime = if (endtime_origin.isEmpty) {
      starttime_origin
    } else {
      endtime_origin
    }

    val pre_extend_params = (row \ "pre_extend_params").asOpt[String].getOrElse("")
    val url = (row \ "wap_url").asOpt[String].getOrElse("")
    val urlref = (row \ "wap_pre_url").asOpt[String].getOrElse("")
    val deviceid = (row \ "deviceid").asOpt[String].getOrElse("")
    val jpid = (row \ "jpid").asOpt[String].getOrElse("")
    val ip = (row \ "ip").asOpt[String].getOrElse("")
    val to_switch = (row \ "to_switch").asOpt[String].getOrElse("0")
    val location = (row \ "location").asOpt[String].getOrElse("")
    val ctag = (row \ "c_label").asOpt[String].getOrElse("")
    val server_jsonstr = (row \ "server_jsonstr").asOpt[String].getOrElse("")

    // =========================================== base to base log ===========================================  //
    val site_id = pageAndEventParser.getSiteId(appName)
    val ref_site_id = site_id
    val gu_id = pageAndEventParser.getGuid(jpid, deviceid, os)
    val terminal_id = pageAndEventParser.getTerminalId(os)

    val gu_create_time = ""

    // =========================================== base to dw ===========================================  //
    var gid = ""
    var uGroup = ""

    val c_server = (row \ "c_server").asOpt[String].getOrElse("")
    if(c_server.nonEmpty)
    {
      val js_c_server = Json.parse(c_server)
      gid = (js_c_server \ "gid").asOpt[String].getOrElse("0")
      uGroup = (js_c_server \ "ugroup").asOpt[String].getOrElse("0")
    }

    val fct_extendParams = pageAndEventParser.getExtendParams(pageName, extendParams)
    val fct_preExtendParams = pageAndEventParser.getExtendParams(pageName, pre_extend_params)

    val forPageId = pageParser.forPageId(pageName, fct_extendParams, server_jsonstr)

    // 154 289 活动页，如果url为空，就直接过滤
    if((forPageId == 154 | forPageId == 289) && url.isEmpty) { return null }

    val forPrePageid = pageParser.forPageId(prePage, fct_preExtendParams, server_jsonstr)

    val (d_page_id: Int, page_type_id: Int, d_page_value: String, d_page_level_id: Int) = dimPage.get(forPageId).getOrElse(0, 0, "", 0)
    val pageId = pageAndEventParser.getPageId(d_page_id, url)
    val pageValue = pageParser.getPageValue(d_page_id, url, fct_extendParams, page_type_id, d_page_value)

    // ref_page_id
    val (d_pre_page_id: Int, d_pre_page_type_id: Int, d_pre_page_value: String, d_pre_page_level_id: Int) = dimPage.get(forPrePageid).getOrElse(0, 0, "", 0)
    val ref_page_id = pageAndEventParser.getPageId(d_pre_page_id, urlref)
    val ref_page_value = pageParser.getPageValue(d_pre_page_id, fct_preExtendParams, urlref, d_pre_page_type_id, d_pre_page_value)

    val parsed_source = pageAndEventParser.getSource(source)
    val shop_id = pageAndEventParser.getShopId(d_page_id, fct_extendParams)
    val ref_shop_id = pageAndEventParser.getShopId(d_pre_page_id, fct_preExtendParams)

    val forLevelId = if(d_page_id == 254 && fct_extendParams.nonEmpty){fCate.get(fct_extendParams).getOrElse("0")} else "0"

    val page_level_id = pageAndEventParser.getPageLevelId(d_page_id, url, d_page_level_id, forLevelId)

    val hot_goods_id = if(d_page_id == 250 && fct_extendParams.nonEmpty && fct_extendParams.contains("_") && fct_extendParams.split("_").length > 2)
    {
      new GetGoodsId().evaluate(fct_extendParams.split("_")(2))
    }
    else {
      ""
    }

    val page_lvl2_value = pageParser.getPageLvl2Value(d_page_id, fct_extendParams, server_jsonstr, url)

    val ref_page_lvl2_value = pageParser.getPageLvl2Value(d_pre_page_id, fct_preExtendParams, server_jsonstr, urlref)

    val pit_type = 0
    val (sortdate, sorthour, lplid, ptplid) = ("", "", "", "")

    val jpk = 0
    val table_source = "mb_page"
    // 最终返回值
    val event_id, event_value, rule_id, test_id, select_id, event_lvl2_value, loadTime, ug_id = ""

    // 根据分区时间来确定
    val (date, hour) = DateUtils.dateHourStr(partitionTime.toLong)

    val user = User.apply(gu_id, uid, utm, gu_create_time, session_id, terminal_id, appVersion, site_id, ref_site_id, ctag, location, jpk, uGroup, date, hour)
    val pe = PageAndEvent.apply(pageId, pageValue, ref_page_id, ref_page_value, shop_id, ref_shop_id, page_level_id, starttime_origin, endTime, hot_goods_id, page_lvl2_value, ref_page_lvl2_value, pit_type, sortdate, sorthour, lplid, ptplid, gid, table_source)
    val page = Page.apply(parsed_source, ip, url, urlref, deviceid, to_switch)
    val event = Event.apply(event_id, event_value, event_lvl2_value, rule_id, test_id, select_id, loadTime, ug_id)

    if (-1 == pageId) {
      println("for_pageid:" + forPageId, " ,page_type_id:" + page_type_id, " ,page_level_id:" + page_level_id,
        " ,pageName:" + pageName, " ,fct_extendParams:" + fct_extendParams,
        " ,page_value:" + pageValue, " ,fct_extendParams:" + fct_extendParams,
        " ,d_page_id:" + d_page_id, " ,d_page_value:" + d_page_value)
      println("page_id=-1===>原始数据为：" + row)
    }

    (user, pe, page, event)
  }
}