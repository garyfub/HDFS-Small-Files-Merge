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
        case ex: Exception => println(ex.printStackTrace())
        println("=======>> Event: parse Exception!!" + "\n======>>异常数据:" + row)
      }

      val ret = if(gu_id.nonEmpty) {
          val endtime = (row \ "endtime").asOpt[String].getOrElse("")
          val server_jsonstr = (row \ "server_jsonstr").asOpt[String].getOrElse("")
          val loadTime = pageAndEventParser.getJsonValueByKey(server_jsonstr, "_t")

          // 如果loadTime非空，就需要判断是否是当天的数据，如果不是，需要过滤掉,因此不需要处理
          if(loadTime.nonEmpty &&
            DateUtils.dateStr(endtime.toLong) != DateUtils.dateStr(loadTime.toLong * 1000)) {
            ("", "", None)
          } else {
            try{
              val res = parse(row, dimpage, dimevent)
              // 过滤异常的数据，具体见解析函数 eventParser.filterOutlierPageId
              if(res == null) {
                ("", "", None)
              }
              else {
                val (user: User, pageAndEvent: PageAndEvent, page: Page, event: Event) = res

                val res_str =  pageAndEventParser.combineTuple(user, pageAndEvent, page, event).map(x=> x match {
                  case y if y == null || y.toString.isEmpty => "\\N"
                  case _ => x
                }).mkString("\001")

                val partitionStr = DateUtils.dateGuidPartitions(endTime, gu_id)
                (partitionStr, "event", res_str)
              }
            }
            catch {
              //使用模式匹配来处理异常
              case ex: Exception => println(ex.printStackTrace())
                println("=======>> Event: parse Exception!!" + "\n======>>异常数据:" + row)
                ("", "", None)
            }
          }
      } else {
        println("=======>> Page: GU_ID IS NULL!!" + "\n======>>异常数据:" + row)
        ("", "", None)
      }
      ret
    } else {
      println("=======>> Page: ROW IS NULL!!" + "\n======>>异常数据:" + row)
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
    val app_version = (row \ "app_version").asOpt[String].getOrElse("0")
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

    val flag = eventParser.filterOutlierPageId(pagename, cid, f_page_extend_params)
    if (flag) {
      // 如果需要过滤，本条数据解析致此结束
      return null
    }

    val for_pageid = eventParser.getForPageId(cid, f_page_extend_params, pagename)

    val for_pre_pageid = eventParser.getForPrePageId(pagename, f_pre_extend_params, pre_page)

    val for_eventid = eventParser.getForEventId(cid, activityname, t_extend_params)

    val rule_id = getAbinfo(extend_params, "rule_id")
    val test_id = getAbinfo(extend_params, "test_id")
    val select_id = getAbinfo(extend_params, "select")

    val (pit_type, gsort_key) = pageAndEventParser.getGsortPit(server_jsonstr)

    val (sortdate, sorthour, lplid, ptplid) = pageAndEventParser.getGsortKey(gsort_key)

    // --------------------------------------------------------------------> event_reg ------------------------------------------------------------------
    val (d_event_id: Int, event_type_id: Int) = dimevent.get(for_eventid).getOrElse(0, 0)
    val event_id = eventParser.getEventId(d_event_id, app_version) + ""
    val event_value = eventParser.getEventValue(event_type_id, activityname, f_extend_params, server_jsonstr)

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

    // TODO 测试代码，测试后需要删掉
    if(-1 == page_id){
      println("for_pageid:" + for_pageid, " ,page_type_id:" + page_type_id, " ,page_level_id:" + page_level_id ,
        " ,page_value:" + page_value , " ,f_page_extend_params:" + f_page_extend_params,
        " ,d_page_id:" + d_page_id, " ,d_page_value:" + d_page_value,
        " ,for_eventid:" + for_eventid,
        " ,d_event_id:" + d_event_id , " ,event_type_id:" + event_type_id, " ,event_id:" + event_id, " ,event_value:" + event_value)
      println("page_id=-1, 原始数据为：" + row)
    }

    (user, pe, page, event)
  }

  def getAbinfo(extend_params: String, arg: String): String = {
    if (extend_params.contains(arg)) {
      val ab_info = pageAndEventParser.getJsonValueByKey(extend_params, "ab_info")
      pageAndEventParser.getJsonValueByKey(ab_info, arg)
    }
    else ""
  }

    /**
      *
      * @param activityname
      * @return
      */
    def getActivityid(activityname: String): Int = {
      new GetMbActionId().evaluate(activityname.toLowerCase())
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