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

    val line = """{"activityname":"click_cube_goods","app_name":"zhe","app_version":"4.1.1","c_label":"C3","c_server":"{\"gid\":\"C3\",\"ugroup\":\"222\"}","cube_position":"1_1","deviceid":"49A3FDC8-2501-4250-8A06-BAF463A008E6","endtime":"1475052337104","endtime_origin":"1475052335999","extend_params":"","ip":"180.156.82.107","jpid":"ba67668beaf7eb8c00cc854dff02e3490b6a890c","location":"","os":"iOS","os_version":"9.3.2","page_extends_param":"345","pagename":"page_tab","pre_extends_param":"316","pre_page":"page_tab","result":"1","server_jsonstr":"{\"pit_info\":\"goods::21761493::1_1\",\"ab_attr\":\"9\",\"cid\":345,\"_t\":1475052281,\"_gsort_key\":\"DEFAULT_SORT_221_20160928_16_276\",\"_pit_type\":3}","session_id":"1448354623610_zhe_1475052328304","source":"","starttime":"1475052337104","starttime_origin":"1475052335999","ticks":"1448354623610","to_switch":"0","uid":"0","utm":"101431"}"""
    val line1 = """{"activityname":"click_cube_banner","app_name":"zhe","app_version":"4.1.0","c_label":"C2","c_server":"{\"gid\":\"C2\",\"ugroup\":\"115_224_243\"}","cube_position":"1_3","deviceid":"861615037481218","endtime":"1475052351492","endtime_origin":"1475052321854","extend_params":"","ip":"117.136.36.196","jpid":"ffffffff-b60d-fa7e-ffff-ffff841f652b","location":"河南省","os":"android","os_version":"5.1.1","page_extends_param":"314","pagename":"page_tab","pre_extends_param":"all","pre_page":"page_tab","result":"1","server_jsonstr":"{\"ads_id\":\"2749\",\"user_group_id\":\"\",\"cid\":314,\"_t\":1475052313}","session_id":"1472712297918_zhe_1475052296593","source":"","starttime":"1475052351492","starttime_origin":"1475052321854","ticks":"1472712297918","to_switch":"0","uid":"42145963","utm":"103489"}"""
    val line3 = """{"activityname":"click_cube_banner","app_name":"zhe","app_version":"4.0.0","c_label":"C2","c_server":"{\"gid\":\"C2\",\"ugroup\":\"327_225_331_326_236\"}","cube_position":"1_1","deviceid":"0","endtime":"1475113780449","endtime_origin":"1475113780301","extend_params":"","gj_ext_params":"348,348,316,http://brand.juanpi.com/1486486?shop_id=1911887&mobile=1&qmshareview=1&qminkview=1","gj_page_names":"page_tab,page_tab,page_tab,page_active","ip":"1.198.234.140","jpid":"ffffffff-a102-63f3-ffff-ffffcb2942bc","location":"河南省","os":"android","os_version":"5.1","page_extends_param":"316","pagename":"page_tab","pre_extends_param":"http://brand.juanpi.com/1486486?shop_id=1911887&mobile=1&qmshareview=1&qminkview=1","pre_page":"page_active","result":"1","server_jsonstr":"{\"ads_id\":\"2761\",\"user_group_id\":\"\",\"cid\":316,\"_t\":1475113687}","session_id":"1469965231946_zhe_1475113610528","source":"","starttime":"1475113780449","starttime_origin":"1475113780301","ticks":"1469965231946","to_switch":"0","uid":"41160683","utm":"101225"}"""
    val line4 = """{"activityname":"click_cube_goods","app_name":"zhe","app_version":"4.1.0","c_label":"C1","c_server":"{\"gid\":\"C1\",\"ugroup\":\"115_224_243_236\"}","cube_position":"16_8","deviceid":"862979034874174","endtime":"1475117887753","endtime_origin":"1475117887514","extend_params":"","ip":"120.195.151.16","jpid":"ffffffff-b757-75ca-ffff-ffffc6b64cea","location":"江苏省","os":"android","os_version":"5.1.1","page_extends_param":"312","pagename":"page_tab","pre_extends_param":"21281775","pre_page":"page_temai_goods","result":"1","server_jsonstr":"{\"pit_info\":\"goods::23051745::16_8\",\"ab_attr\":\"7\",\"cid\":312,\"_t\":1475117875,\"_gsort_key\":\"DEFAULT_SORT_171_20160929_10_287\",\"_pit_type\":3}","session_id":"1474240909599_zhe_1475117146716","source":"","starttime":"1475117887753","starttime_origin":"1475117887514","ticks":"1474240909599","to_switch":"0","uid":"43632600","utm":"103489"}"""
    val line5 = """{"activityname":"click_cube_goods","app_name":"zhe","app_version":"4.1.1","c_label":"C3","c_server":"{\"gid\":\"C3\",\"ugroup\":\"222\"}","cube_position":"4_5","deviceid":"868978026454320","endtime":"1475117905514","endtime_origin":"1475117904518","extend_params":"","ip":"183.214.15.13","jpid":"ffffffff-b9e9-7de2-ffff-ffff8b51bac0","location":"湖南省","os":"android","os_version":"4.4.4","page_extends_param":"314","pagename":"page_tab","pre_extends_param":"1704507_1370835_28481534","pre_page":"page_home_brand_in","result":"1","server_jsonstr":"{\"pit_info\":\"brand::1723573::4_5\",\"hot_goods_id\":\"24871441\",\"ab_attr\":\"8\",\"cid\":314,\"_t\":1475117875,\"_gsort_key\":\"POSTION_SORT_177_20160929_10_209_187\",\"_pit_type\":10}","session_id":"1475112653168_zhe_1475116418699","source":"","starttime":"1475117905514","starttime_origin":"1475117904518","ticks":"1475112653168","to_switch":"0","uid":"0","utm":"105727"}"""
    val line6 = """{"activityname":"click_temai_inpage_shoppingbag","app_name":"zhe","app_version":"3.4.3","c_label":"C1","cube_position":"","deviceid":"865242028274183","endtime":"1475117926998","endtime_origin":"1475117926602","extend_params":"29071904","gj_ext_params":"1856599_1950855_27251833,all,http://m.juanpi.com/zhuanti/nxjm?mobile=1&qminkview=1&qmshareview=1,http://m.juanpi.com/zhuanti/nxjm?mobile=1&qminkview=1&qmshareview=1","gj_page_names":"page_home_brand_in,page_tab,page_active,page_active","ip":"124.113.11.229","jpid":"ffffffff-bd0f-38dd-ffff-ffffc4fa80c4","location":"安徽省","os":"android","os_version":"4.4.4","page_extends_param":"29071904","pagename":"","pre_extends_param":"http://m.juanpi.com/zhuanti/nxjm?mobile=1&qminkview=1&qmshareview=1","pre_page":"page_active","result":"1","server_jsonstr":"{}","session_id":"1463488981675_zhe_1475117397989","source":"","starttime":"1475117926998","starttime_origin":"1475117926602","ticks":"1463488981675","to_switch":"0","uid":"37610036","utm":"101218"}"""

    val row = Json.parse(line6)
    val pagename = (row \ "pagename").asOpt[String].getOrElse("").toLowerCase()
    val page_extends_param = (row \ "page_extends_param").asOpt[String].getOrElse("")
    val f_page_extend_params = pageAndEventParser.getExtendParams(pagename, page_extends_param)
    println(pagename, page_extends_param, f_page_extend_params)

    val server_jsonstr = (row \ "server_jsonstr").asOpt[String].getOrElse("")
    println("server_jsonstr:" + server_jsonstr)

    val activityname = (row \ "activityname").asOpt[String].getOrElse("").toLowerCase()
    val extend_params = (row \ "extend_params").asOpt[String].getOrElse("")
    val app_version = (row \ "app_version").asOpt[String].getOrElse("0")
    val ep = eventParser.getExtendParamsFromBase(activityname, extend_params, app_version)
    println("ep", ep)

    val d_page_id = 1
    val page_id = pageAndEventParser.getPageId(d_page_id, f_page_extend_params)
    val cid = pageAndEventParser.getJsonValueByKey(server_jsonstr, "cid")
    println("cid:", cid)
    if(!cid.isEmpty)
      {
        println(cid.toInt)
      }

    val for_pageid = eventParser.getForPageId(cid, f_page_extend_params, pagename)
    println("for_pageid" ,for_pageid)

    println(extend_params, app_version)
    val t_extend_params = eventParser.getExtendParamsFromBase(activityname, extend_params, app_version)
    val cube_position = (row \ "cube_position").asOpt[String].getOrElse("")
    println(activityname, t_extend_params, cube_position)
    val f_extend_params = eventParser.getForExtendParams(activityname, t_extend_params, cube_position, server_jsonstr)
    println(f_extend_params)

    val deviceid = (row \ "deviceid").asOpt[String].getOrElse("")
    println(deviceid)

    val (pit_type, gsort_key) = pageAndEventParser.getGsortPit(server_jsonstr)
    println(gsort_key)
    println(pageAndEventParser.getGsortKey(gsort_key))

    val forPageId = pageAndEventParser.forPageId(pagename, extend_params, server_jsonstr)
    println("forPageId:",forPageId)

//    val event_value = eventParser.getEventValue(event_type_id, activityname, f_extend_params, server_jsonstr)
  }
}
