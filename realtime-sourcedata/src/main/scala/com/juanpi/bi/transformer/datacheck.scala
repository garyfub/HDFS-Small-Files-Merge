package com.juanpi.bi.transformer

import play.api.libs.json.Json

/**
  * Created by gongzi on 2016/9/28.
  */
object datacheck {

  def testcid(server_jsonstr: String): Any = {
    if (server_jsonstr.contains("cid")) {
      val js_server_jsonstr = Json.parse(server_jsonstr)
      val cid = (js_server_jsonstr \ "cid").asOpt[String].getOrElse("")
      println(cid)
    }
  }

  def main(args: Array[String]) {
    val row = """{"activityname":"click_cube_goods","app_name":"zhe","app_version":"4.1.0","c_label":"C2","c_server":"{\"gid\":\"C2\",\"ugroup\":\"225_331_326_236\"}","cube_position":"1_20","deviceid":"860410038070330","endtime":"1475049912441","endtime_origin":"1475049911460","extend_params":"","ip":"223.96.150.217","jpid":"00000000-594b-ee3a-ffff-ffffbb883962","location":"山东省","os":"android","os_version":"5.0.2","page_extends_param":"312","pagename":"page_tab","pre_extends_param":"1165420_1880845_22941538","pre_page":"page_home_brand_in","result":"1","server_jsonstr":"{\"pit_info\":\"brand::1343506::1_20\",\"hot_goods_id\":\"20880309\",\"ab_attr\":\"0\",\"cid\":312,\"_t\":1475049690,\"_gsort_key\":\"DEFAULT_SORT_171_20160928_16_287\",\"_pit_type\":10}","session_id":"1472443797881_zhe_1475049684332","source":"","starttime":"1475049912441","starttime_origin":"1475049911460","ticks":"1472443797881","to_switch":"0","uid":"39617683","utm":"101224"}"""
    val d_page_id = 1
    val f_page_extend_params = "314"
    pageAndEventParser.getPageId(d_page_id, f_page_extend_params)
    val page_id = -1
    if("-1" == page_id) println("hhhhhhhhh")
  }
}
