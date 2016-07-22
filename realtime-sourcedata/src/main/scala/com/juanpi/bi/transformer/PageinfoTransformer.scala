package com.juanpi.bi.transformer

import java.util.regex.Pattern

import com.juanpi.bi.hiveUDF._
import com.juanpi.bi.sc_utils.DateUtils
import com.juanpi.bi.streaming.DateHour
import org.apache.hadoop.hbase.client.{Get, Put, Result}
import play.api.libs.json.{JsValue, Json}
import org.apache.hadoop.hbase.util.Bytes
import com.juanpi.bi.init.InitConfig.initTicksHistory

import scala.collection.mutable
/**
  * 解析逻辑的具体实现
  */
class PageinfoTransformer extends ITransformer {

  val HbaseFamily = "dw"

  def parse(row: JsValue, dimpage: mutable.HashMap[String, (Int, Int, String, Int)]): String = {
    // mb_pageinfo
    val ticks = (row \ "ticks").asOpt[String].getOrElse("")
    val session_id = (row \ "session_id").asOpt[String].getOrElse("")
    val pagename = (row \ "pagename").asOpt[String].getOrElse("").toLowerCase()
    val starttime = (row \ "starttime").asOpt[String].getOrElse("0")
    val endtime = (row \ "endtime").asOpt[String].getOrElse(0)
    val pre_page = (row \ "pre_page").asOpt[String].getOrElse("")
    val uid = (row \ "uid").asOpt[String].getOrElse("0")
    val extend_params = (row \ "extend_params").asOpt[String].getOrElse("")
    val app_name = (row \ "app_name").asOpt[String].getOrElse("")
    val app_version = (row \ "app_version").asOpt[String].getOrElse("")
//    val os_version = (row \ "os_version").asOpt[String].getOrElse("")
    val os = (row \ "os").asOpt[String].getOrElse("")
    var utm = (row \ "utm").asOpt[String].getOrElse("0")
    val source = (row \ "source").asOpt[String].getOrElse("")
    val starttime_origin = (row \ "starttime_origin").asOpt[String].getOrElse("")
    val endtime_origin = (row \ "endtime_origin").asOpt[String].getOrElse("")
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
    val site_id = pageAndEventParser.getSiteId(app_name)
    val ref_site_id = site_id
    val gu_id = pageAndEventParser.getGuid(jpid, deviceid, os)
    val terminal_id = pageAndEventParser.getTerminalId(os)

    var gu_create_time = ""

    // 查hbase 从 ticks_history 中查找 ticks 存在的记录
    // 查询某条数据
//    val ticks_history = initTicksHistory()
//    val key = new Get(Bytes.toBytes(gu_id + ":" + utm))
//    println("=======> ticks_history.get:" + key)
//    val ticks_res = ticks_history.get(key)
//
//    if (!ticks_res.isEmpty) {
//      utm = Bytes.toString(ticks_res.getValue(HbaseFamily.getBytes, "utm".getBytes))
//      gu_create_time = Bytes.toString(ticks_res.getValue(HbaseFamily.getBytes, "starttime".getBytes))
//    }
//    else {
//      // 如果不存在就写入 hbase
//      // 准备插入一条 key 为 id001 的数据
//      val p = new Put((ticks + app_name).getBytes)
//      // 为put操作指定 column 和 value （以前的 put.add 方法被弃用了）
//      p.addColumn(HbaseFamily.getBytes, "utm".getBytes, utm.getBytes)
//      p.addColumn(HbaseFamily.getBytes, "init_date".getBytes, gu_create_time.getBytes)
//      //提交
//      ticks_history.put(p)
//    }

    // =========================================== base to dw ===========================================  //
    // 用户画像中定义的
    var gid = 0
    var ugroup = 0

    val c_server = (row \ "c_server").asOpt[String].getOrElse("")
    if(!c_server.isEmpty())
    {
      val js_c_server = Json.parse(c_server)
      gid = (js_c_server \ "gid").asOpt[Int].getOrElse(0)
      ugroup = (js_c_server \ "ugroup").asOpt[Int].getOrElse(0)
    }

    // mb_pageinfo -> mb_pageinfo_log
    val extendParams1 = pageAndEventParser.getExtendParams(pagename, extend_params)
    val preExtendParams1 = pageAndEventParser.getExtendParams(pagename, pre_extend_params)

    // for_pageid 判断
    val for_pageid = pageAndEventParser.forPageId(pagename, extendParams1, server_jsonstr)
    val for_pre_pageid = pageAndEventParser.forPageId(pre_page, preExtendParams1, server_jsonstr)

    val (d_page_id: Int, page_type_id: Int, d_page_value: String, d_page_level_id: Int) = dimpage.get(for_pageid).getOrElse(0, 0, "", 0)
    val page_id = pageAndEventParser.getPageId(d_page_id, extendParams1)
    val page_value = pageAndEventParser.getPageValue(d_page_id, extendParams1, page_type_id, d_page_value)

    // ref_page_id
    val (d_pre_page_id: Int, d_pre_page_type_id: Int, d_pre_page_value: String, d_pre_page_level_id: Int) = dimpage.get(for_pre_pageid).getOrElse(0, 0, "", 0)
    val ref_page_id = pageAndEventParser.getPageId(d_pre_page_id, preExtendParams1)
    val ref_page_value = pageAndEventParser.getPageValue(d_pre_page_id, preExtendParams1, d_pre_page_type_id, d_pre_page_value)

    val parsed_source = pageAndEventParser.getSource(source)
    val shop_id = pageAndEventParser.getShopId(d_page_id, extendParams1)
    val ref_shop_id = pageAndEventParser.getShopId(d_pre_page_id, preExtendParams1)

    val page_level_id = pageAndEventParser.getPageLevelId(d_page_id, extendParams1, d_page_level_id)

    // WHEN p1.page_id = 250 THEN getgoodsid(NVL(split(a.extendParams1,'_')[2],''))
    val hot_goods_id = if(d_page_id == 250 && !extendParams1.isEmpty && extendParams1.contains("_") && extendParams1.split("_").length > 2)
    {
      new GetGoodsId().evaluate(extendParams1.split("_")(2))
    }
    else {""}

    val page_lvl2_value = pageAndEventParser.getPageLvl2Value(d_page_id, extendParams1, server_jsonstr)

    val ref_page_lvl2_value = pageAndEventParser.getPageLvl2Value(d_pre_page_id, preExtendParams1, server_jsonstr)

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

    val jpk = 0
    val tab_source = "page"
    // 最终返回值
    val event_id,event_value,rule_id,test_id,select_id,event_lvl2_value,loadTime = ""

    println("======>> page_id :: " + page_id)

    Array(terminal_id,app_version,gu_id,utm,site_id,ref_site_id,uid,session_id,deviceid,page_id,
      page_value,ref_page_id,ref_page_value,page_level_id,page_lvl2_value,ref_page_lvl2_value,jpk,pit_type,sortdate,
      sorthour,lplid,ptplid,gid,ugroup,shop_id,ref_shop_id,starttime,endtime,hot_goods_id,ctag,location,ip,url,urlref,
      to_switch,parsed_source,event_id,event_value,rule_id,test_id,select_id,event_lvl2_value,loadTime,gu_create_time,tab_source
      //      ,date,hour
    ).mkString("\u0001")

  }

  // 返回解析的结果
  def transform(line: String, dimpage: mutable.HashMap[String, (Int, Int, String, Int)]): (String, String) = {

    // fastjson 也可以用。
    // val row = JSON.parseObject(line)

    //play
    val row = Json.parse(line)

    println("===#transform#===>> row:: " + row)

    if (row != null) {
      // 解析逻辑
      val res = parse(row, dimpage)

      // for test
//      val res = Array("2", "2", "4", "5", "7", "8", "10","11","12").mkString("\u0001")

      (DateUtils.dateHour((row \ "endtime").as[String].toLong).toString, res)
    } else {
      (DateHour("1970-01-01", "1").toString, line)
    }
  }

}

// for test
object PageinfoTransformer{
  def main(args: Array[String]) {

    val  pp: PageinfoTransformer = new PageinfoTransformer()
    val liuliang =
      """
        |{"app_name":"zhe","app_version":"3.4.6","c_label":"C3","c_server":"{\"gid\":\"C3\",\"ugroup\":\"143_223_112_142\"}","deviceid":"867568022962029","endtime":"1468929132822","endtime_origin":"1468929131796","extend_params":"crazy_zhe","gj_ext_params":"past_zhe,1580540_1500762_13504152,past_zhe,crazy_zhe","gj_page_names":"page_tab,page_home_brand_in,page_tab,page_tab","ip":"119.109.179.179","jpid":"ffffffff-bc21-7da8-ffff-ffffe4de7969","location":"辽宁省","os":"android","os_version":"4.4.4","pagename":"page_tab","pre_extend_params":"past_zhe","pre_page":"page_tab","server_jsonstr":"{\"ab_info\":{\"rule_id\":\"\",\"test_id\":\"\",\"select\":\"\"},\"ab_attr\":\"7\"}","session_id":"1468155168409_zhe_1468929047609","source":"","starttime":"1468929130209","starttime_origin":"1468929129183","ticks":"1468155168409","to_switch":"0","uid":"40102432","utm":"104954","wap_pre_url":"","wap_url":""}
        |""".stripMargin

    val line = Json.parse(liuliang.replaceAll("null", """\\"\\""""))
//    val p = pp.parse(line)
//    println(p)

    val aa = (Json.parse("{}") \ "order_status").asOpt[String].getOrElse("")
    println(aa)

    println(if("a_b".split("_").length > 2) {"a_b_c".split("_")(2)})

  }
}