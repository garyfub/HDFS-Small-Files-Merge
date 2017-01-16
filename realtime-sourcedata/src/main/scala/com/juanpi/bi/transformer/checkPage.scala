package com.juanpi.bi.transformer

import play.api.libs.json.Json

/**
  * Created by gongzi on 2016/9/30.
  */
object checkPage {

  val line = "{\"app_name\":\"zhe\",\"app_version\":\"4.2.3\",\"c_label\":\"C2\",\"c_server\":\"{\\\"gid\\\":\\\"C2\\\",\\\"ugroup\\\":\\\"580_486_485_496_495_335_453_457_377_574_547_608_584_572_607_615_618_593\\\"}\",\"deviceid\":\"869949029295654\",\"endtime\":\"1484287265361\",\"endtime_origin\":\"1484287265191\",\"extend_params\":\"http://m.juanpi.com/faxian\",\"ip\":\"58.255.228.172\",\"jpid\":\"00000000-0000-0030-f7f1-c2bb00000030\",\"location\":\"广东省茂名市茂南区油城六路三巷靠近裕丰花园(油城六路三巷)\",\"os\":\"android\",\"os_version\":\"6.0\",\"pagename\":\"page_h5\",\"pre_extend_params\":\"all\",\"pre_page\":\"page_tab\",\"server_jsonstr\":\"{}\",\"session_id\":\"1472284730622_zhe_1484286985482\",\"source\":\"\",\"starttime\":\"1484287265209\",\"starttime_origin\":\"1484287265039\",\"ticks\":\"1472284730622\",\"to_switch\":\"0\",\"uid\":\"32346127\",\"utm\":\"101225\",\"wap_pre_url\":\"\",\"wap_url\":\"\"}"

  def testcid(server_jsonstr: String): Any = {
    if (server_jsonstr.contains("cid")) {
      val js_server_jsonstr = Json.parse(server_jsonstr)
      val cid = (js_server_jsonstr \ "cid").asOpt[String].getOrElse("")
      println(cid)
    }
  }

  def main(args: Array[String]): Unit = {

    val pageName = "page_h5"
    val extendParams = "http://mact.juanpi.com/limitsell2?qminkview=1&qmshareview=1"
    val server_jsonstr = ""
    val fct_extendParams = pageAndEventParser.getExtendParams(pageName, extendParams)
    val forPageId = pageParser.forPageId(pageName, fct_extendParams, server_jsonstr)

    // (http://mact.juanpi.com/limitsell2?qminkview=1&qmshareview=1,page_h5)
    println(fct_extendParams, forPageId)

    val d_page_id = 289

//    val forLevelId = if(d_page_id == 254 && fct_extendParams.nonEmpty){fCate.get(fct_extendParams).getOrElse("0")} else "0"
    val d_page_level_id = 0
    val forLevelId = "0"
    val page_level_id = pageAndEventParser.getPageLevelId(d_page_id, fct_extendParams, d_page_level_id, forLevelId)
//
    val url = "http://mact.juanpi.com/limitsell2?qminkview=1&qmshareview=1"
//
    val pageId = pageAndEventParser.getPageId(d_page_id, url)
    println(pageId, page_level_id)

  }
}
