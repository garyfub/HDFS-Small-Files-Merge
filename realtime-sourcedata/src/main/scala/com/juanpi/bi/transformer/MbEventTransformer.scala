package com.juanpi.bi.transformer

import com.juanpi.bi.bean.{Event, Page, PageAndEvent, User}
import com.juanpi.bi.hiveUDF.{GetGoodsId, GetMbActionId, GetPageID}
import com.juanpi.bi.sc_utils.DateUtils
import play.api.libs.json.{JsValue, Json}

import scala.collection.mutable

/**
  * Created by gongzi on 2016/7/11.
  */
class MbEventTransformer extends ITransformer {

  /**
    *
    * @param line
    * @param dimpage
    * @param dimevent
    * @return
    */
  def logParser(line: String,
                dimpage: mutable.HashMap[String, (Int, Int, String, Int)],
                dimevent: mutable.HashMap[String, (Int, Int)]): (String, String, Any) = {

    val row = Json.parse(line)
    val ticks = (row \ "ticks").asOpt[String].getOrElse("")
    val jpid = (row \ "jpid").asOpt[String].getOrElse("")
    val deviceId = (row \ "deviceid").asOpt[String].getOrElse("")
    val os = (row \ "os").asOpt[String].getOrElse("")
    val endTime = (row \ "endtime").as[String].toLong

    // TODO 逻辑待优化
    if (ticks.length() >= 13) {
      // 解析逻辑
      var gu_id = ""
      try {
        gu_id = pageAndEventParser.getGuid(jpid, deviceId, os)
      } catch {
        //使用模式匹配来处理异常
        case ex: Exception => println(ex.printStackTrace() + "==>>gu_id======>>异常数据:" + row)
      }

//      println("=======>> ticks=" + ticks + "#, jpid=" + jpid + "#, deviceid=" + deviceId + "#, os=" + os + "#, gu_id=" + gu_id + "#, endtime=" + endTime)

      val ret = if(gu_id.nonEmpty) {
//        try {
          val endtime = (row \ "endtime").asOpt[String].getOrElse("")
          val server_jsonstr = (row \ "server_jsonstr").asOpt[String].getOrElse("")
          val loadTime = pageAndEventParser.getJsonValueByKey(server_jsonstr, "_t")

          // 如果loadTime非空，就需要判断是否是当天的数据，如果不是，需要过滤掉,因此不需要处理
          if(loadTime.nonEmpty &&
            DateUtils.dateStr(endtime.toLong) != DateUtils.dateStr(loadTime.toLong * 1000)) {
            ("", "", None)
          } else {
            val (user: User, pageAndEvent: PageAndEvent, page: Page, event: Event) = parse(row, dimpage, dimevent)
            val res_str =  pageAndEventParser.combineTuple(user, pageAndEvent, page, event).map(x=> x match {
              case y if y == null || y.toString.isEmpty => "\\N"
              case _ => x
            }).mkString("\001")
            val partitionStr = DateUtils.dateGuidPartitions(endTime, gu_id)
            (partitionStr, "event", res_str)
          }
      } else {
        ("", "", None)
      }
      ret
    } else {
      ("", "", None)
    }
  }

  def parse(row: JsValue,
            dimpage: mutable.HashMap[String, (Int, Int, String, Int)],
            dimevent: mutable.HashMap[String, (Int, Int)]): (User, PageAndEvent, Page, Event) = {

    // ---------------------------------------------------------------- mb_event ----------------------------------------------------------------
    val session_id = (row \ "session_id").asOpt[String].getOrElse("")
    val activityname = (row \ "activityname").asOpt[String].getOrElse("").toLowerCase()
    val starttime = (row \ "starttime").asOpt[String].getOrElse("")
    val endtime = (row \ "endtime").asOpt[String].getOrElse("")
    val result = (row \ "result").asOpt[String].getOrElse("")
    val uid = (row \ "uid").asOpt[String].getOrElse("0")
    val extend_params = (row \ "extend_params").asOpt[String].getOrElse("")
    // utm 的值还会改变，故定义成var
    val utm = (row \ "utm").asOpt[String].getOrElse("")
    val source = ""

    val app_name = (row \ "app_name").asOpt[String].getOrElse("")
    val app_version = (row \ "app_version").asOpt[String].getOrElse("")
    val os = (row \ "os").asOpt[String].getOrElse("")
    val pagename = (row \ "pagename").asOpt[String].getOrElse("").toLowerCase()
    val page_extends_param = (row \ "page_extends_param").asOpt[String].getOrElse("")
    val deviceid = (row \ "deviceid").asOpt[String].getOrElse("")
    val pre_page = (row \ "pre_page").asOpt[String].getOrElse("")
    // 字段与pageinfo中的不太一样
    val pre_extends_param = (row \ "pre_extends_param").asOpt[String].getOrElse("")
    val jpid = (row \ "jpid").asOpt[String].getOrElse("")
    val ip = ""
    val to_switch = (row \ "to_switch").asOpt[String].getOrElse("")
    val cube_position = (row \ "cube_position").asOpt[String].getOrElse("")
    val location = (row \ "location").asOpt[String].getOrElse("")
    val ctag = (row \ "c_label").asOpt[String].getOrElse("")
    val server_jsonstr = (row \ "server_jsonstr").asOpt[String].getOrElse("")

    val loadTime = pageAndEventParser.getJsonValueByKey(server_jsonstr, "_t")

    // 用户画像中定义的
    val c_server = (row \ "c_server").asOpt[String].getOrElse("")
    val (gid, ugroup) = if(c_server.nonEmpty) {
      val js_c_server = Json.parse(c_server)
      val gid = (js_c_server \ "gid").asOpt[String].getOrElse("0")
      val ugroup = (js_c_server \ "ugroup").asOpt[String].getOrElse("0")
      (gid, ugroup)
    } else {
      ("0", "0")
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
          val ads_id = (js_t_extend_params \ "ads_id").toString()
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
      // when cast(get_json_object(server_jsonstr, '$.cid') as int) > 0 or (cast(get_json_object(server_jsonstr, '$.cid') as int) = -100 and page_extends_param in (10045,100105)) then 'page_tab'
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
        val pid = new GetPageID().evaluate(f_pre_extend_params).toInt
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
    } else if (!"click_navigation".equalsIgnoreCase(activityname)) {
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
      case "360" => pageAndEventParser.getJsonValueByKey(server_jsonstr, "item")
      case "482"|"481"|"480"|"479" => pageAndEventParser.getJsonValueByKey(server_jsonstr, "_rmd")
      case _ => ""
    }

    val jpk = 0

    val (date, hour) = DateUtils.dateHourStr(endtime.toLong)
    val table_source = "mb_event"

    val user = User.apply(gu_id, uid, utm, "", session_id, terminal_id, app_version, site_id, ref_site_id, ctag, location, jpk, ugroup, date, hour)
    val pe = PageAndEvent.apply(page_id, page_value, ref_page_id, ref_page_value, shop_id, ref_shop_id, page_level_id, starttime, endtime, hot_goods_id, page_lvl2_value, ref_page_lvl2_value, pit_type, sortdate, sorthour, lplid, ptplid, gid, table_source)
    val page = Page.apply(source, ip, "", "", deviceid, to_switch)
    val event = Event.apply(event_id, event_value, event_lvl2_value, rule_id, test_id, select_id, loadTime)
//    println("======> for_eventid:"+ for_eventid + "#, event=" + event + "#, for_pageid=" + for_pageid + ",page_id=" + page_id + "=======>> server_jsonstr::", server_jsonstr)
    (user, pe, page, event)
  }

  def getEventId(d_event_id: Int, app_version: String): Int = {
    val app_ver = getVersionNum(app_version)
    val eid = d_event_id match {
      case a if (d_event_id == 0) => -1
      case b if (app_ver > 323 || d_event_id != 279) => d_event_id
      case _ => -999
    }
    eid
  }

  def getEventValue(event_type_id: Int, activityname: String, extend_params: String, server_jsonstr: String): String =
  {
      //  TODO -- gognzi && lielie,过滤掉商品流坑位数据中非当天的数据
      val oper_time = pageAndEventParser.getJsonValueByKey(server_jsonstr, "_t")

      // TODO
      if (event_type_id == 10) {
        if (activityname.contains("click_cube")) {
          extend_params
        } else if (!server_jsonstr.contains("_t") || oper_time.isEmpty) {
          ""
        } else {
          extend_params
        }
      } else {
        extend_params
      }
  }

    def getAbinfo(extend_params: String, arg: String): String = {
      if (extend_params.contains(arg)) {
        val ab_info = pageAndEventParser.getJsonValueByKey(extend_params, "ab_info")
        pageAndEventParser.getJsonValueByKey(ab_info, arg)
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
      val resInt = if (app_version.nonEmpty) {
        app_version.replace(".", "").toInt
      }
      else {
        0
      }
      resInt
    }
  }

// for test
object MbEventTransformer {

  def main(args: Array[String]) {
    val t = pageAndEventParser.getJsonValueByKey("""{"pit_info":"goods::16915719::2_19","cid":0,"_t":1470639193}""", "_t")
    println(t)
    var cid = ""
    var cid2 = ""
    val server_jsonstr = """{"pit_info":"ad_id::135::block_id::618::img_id::386::3_1","cid":"","_t":1471509688}"""
    if (server_jsonstr.contains("cid")) {
      val js_server_jsonstr = Json.parse(server_jsonstr)
      cid = (js_server_jsonstr \ "cid").asOpt[String].getOrElse("")
      cid2 = (js_server_jsonstr \ "cid").asOpt[String].getOrElse("")
    }
    if(!cid.isEmpty) println(cid.toInt)
    val tu = (t, cid, cid2, 0, null)
    val tu1 = (t, cid)
    val res = pageAndEventParser.combineTuple(tu, tu1).map(x=> x match {
      case null => "\\N"
//      case z if z == null => "\\N"
      case y if y.toString.isEmpty => "\\N"
      case _ => x
    }).mkString("\001")
    println(res)
  }
}