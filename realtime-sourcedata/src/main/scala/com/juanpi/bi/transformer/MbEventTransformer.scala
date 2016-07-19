package com.juanpi.bi.transformer

import com.alibaba.fastjson.JSON
import com.juanpi.bi.hiveUDF.{GetGoodsId, GetMbPageId}
import com.juanpi.bi.init.InitConfig._
import com.juanpi.bi.sc_utils.DateUtils
import com.juanpi.bi.streaming.DateHour
import org.apache.hadoop.hbase.client.{Get, Put}
import play.api.libs.json.{JsValue, Json}

/**
  * Created by gongzi on 2016/7/11.
  */
class MbEventTransformer extends ITransformer{

  def parse(row: JsValue): String = {
    // mb_event
    val ticks = (row \ "ticks").asOpt[String].getOrElse("")
    val session_id = (row \ "session_id").asOpt[String].getOrElse("")
    val activityname = (row \ "activityname").asOpt[String].getOrElse("")
    val starttime = (row \ "starttime").asOpt[String].getOrElse("")
    val endtime = (row \ "endtime").asOpt[String].getOrElse("")
    val result = (row \ "result").asOpt[String].getOrElse("")
    val uid = (row \ "uid").asOpt[String].getOrElse("")
    val extend_params = (row \ "extend_params").asOpt[String].getOrElse("")
    // utm 的值还会改变，故定义成var
    var utm = (row \ "utm").asOpt[String].getOrElse("")
    val source = (row \ "source").asOpt[String].getOrElse("")
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
    val pre_extend_params = (row \ "pre_extends_param").asOpt[String].getOrElse("")
    val gj_page_names = (row \ "gj_page_names").asOpt[String].getOrElse("")
    val gj_ext_params = (row \ "gj_ext_params").asOpt[String].getOrElse("")
    val jpid = (row \ "jpid").asOpt[String].getOrElse("")
    val ip = (row \ "ip").asOpt[String].getOrElse("")
    val to_switch = (row \ "to_switch").asOpt[String].getOrElse("")
    val cube_position = (row \ "cube_position").asOpt[String].getOrElse("")
    val location = (row \ "location").asOpt[String].getOrElse("")
    val c_label = (row \ "c_label").asOpt[String].getOrElse("")
    val server_jsonstr = (row \ "server_jsonstr").asOpt[String].getOrElse("")

    var gid = 0
    var ugroup = 0
    val c_server = (row \ "c_server").asOpt[String].getOrElse("")
    if(!c_server.isEmpty())
    {
      val js_c_server = Json.parse(c_server)
      gid = (js_c_server \ "gid").asOpt[Int].getOrElse(0)
      ugroup = (js_c_server \ "ugroup").asOpt[Int].getOrElse(0)
    }

    val terminal_id = pageAndEventParser.getTerminalId(os)
    val site_id = pageAndEventParser.getSiteId(app_name)
    val ref_site_id = site_id

    // pageid
    {
      // for_pageid 判断
      val for_pageid = forPageId(pagename, extend_params, server_jsonstr)
      val for_pre_pageid = forPageId(pre_page, pre_extend_params, server_jsonstr)
      val p_source = getSource(source)

      val (d_page_id: Int, page_type_id: Int, d_page_value: String, d_page_level_id: Int) = dimPages.get(for_pageid).getOrElse(0, 0, "", 0)
      val page_id = getPageId(d_page_id, extend_params)
      var page_value = getPageValue(d_page_id, extend_params, page_type_id, d_page_value)
    }


    // ref_page_id
    val (d_pre_page_id: Int, d_pre_page_type_id: Int, d_pre_page_value: String, d_pre_page_level_id: Int) = dimPages.get(for_pre_pageid).getOrElse(0, 0, "", 0)
    var ref_page_id = getPageId(d_pre_page_id, pre_extend_params)
    var ref_page_value = getPageValue(d_pre_page_id, pre_extend_params, d_pre_page_type_id, d_pre_page_value)


    // mb_event -> mb_event_log
    val (extend_params_1, pre_extend_params_1) = pagename.toLowerCase() match {
      case "page_goods" | "page_temai_goods" | "page_temai_imagetxtgoods" | "page_temai_parametergoods" => {
        (new GetGoodsId().evaluate(extend_params), new GetGoodsId().evaluate(pre_extend_params))
      }
      case _ => {
        (extend_params.toLowerCase(), pre_extend_params.toLowerCase())
      }
    }

    // TODO GetMbPageId 函数需要更新
    val pageId = GetMbPageId.evaluate(pagename.toLowerCase(), extend_params_1)
    val goodsId = if ((pageId == 158) ||
      (pageId == 159 && (app_version == "3.2.3" || app_version == "3.2.4") && (os.toLowerCase() == "ios"))) {
      extend_params_1
    } else {
      "-1"
    }

    var gu_create_time = ""

    val gu_id = os.toLowerCase match {
      case "android" => jpid
      case "ios" => deviceid
      case _ => "0"
    }

    val logTime = if (starttime.size == 0) {
      0L
    } else {
      starttime.toLong
    }

    val validGoodsId = try {
      if (goodsId.size == 0) {
        "-1"
      } else {
        val goods = goodsId.toInt
        goods.toString
      }
    } catch {
      case ex: NumberFormatException => {
        println("======>> pageinfo解析异常" + ":" + goodsId + ":" + row)
        "-1"
      }
    }

    // gu_id
    val id = if ((uid.length == 0) || uid.equals("0")) {
      gu_id
    } else {
      uid
    }
    Array(terminal_id,app_version,gu_id,utm,site_id,ref_site_id,uid,session_id,deviceid,page_id,
      page_value,ref_page_id,ref_page_value,page_level_id,page_lvl2_value,ref_page_lvl2_value,jpk,pit_type,sortdate,
      sorthour,lplid,ptplid,gid,ugroup,shop_id,ref_shop_id,starttime,endtime,hot_goods_id,ctag,location,ip,url,urlref,
      to_switch,source,event_id,event_value,rule_id,test_id,select_id,event_lvl2_value,loadTime,gu_create_time,tab_source
      //      ,date,hour
    ).mkString("\u0001")

  }

  // 返回解析的结果
//  def transform(line: String, page: RDD[(Int, (String, String))], event: RDD[(Int, (String, String))]): (String, String) = {
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

    val row = Json.parse(event)
    println(row)
  }
}