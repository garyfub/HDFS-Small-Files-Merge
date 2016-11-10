package com.juanpi.bi.transformer

import com.juanpi.hive.udf.GetPageID
import play.api.libs.json.Json

/**
  * Created by gongzi on 2016/9/30.
  */
object checkPage {

  def testcid(server_jsonstr: String): Any = {
    if (server_jsonstr.contains("cid")) {
      val js_server_jsonstr = Json.parse(server_jsonstr)
      val cid = (js_server_jsonstr \ "cid").asOpt[String].getOrElse("")
      println(cid)
    }
  }

  def main(args: Array[String]) {

    val  pageStr = """{"app_name":"zhe","app_version":"4.1.2","c_label":"C2","c_server":"{\"gid\":\"C2\",\"ugroup\":\"243_217_334\"}","deviceid":"862393034473666","endtime":"1478569460450","endtime_origin":"1478569460250","extend_params":"","ip":"223.104.169.202","jpid":"ffffffff-829c-a922-d5a0-2a3e2429bc6d","location":"","os":"android","os_version":"5.1.1","pagename":"page_active","pre_extend_params":"21385396","pre_page":"page_temai_goods","server_jsonstr":"{}","session_id":"1476796038532_zhe_1478569202593","source":"","starttime":"1478569452229","starttime_origin":"1478569452029","ticks":"1476796038532","to_switch":"0","uid":"37342629","utm":"103489","wap_pre_url":"","wap_url":"http://m.juanpi.com/shop/factivity/1979775?mobile=1&actid=1979775"}"""
//    val row = Json.parse(pageStr)
//    val session_id = (row \ "session_id").asOpt[String].getOrElse("")
//    val pagename = (row \ "pagename").asOpt[String].getOrElse("").toLowerCase()
//    val starttime = (row \ "starttime").asOpt[String].getOrElse("0")
//    val endtime = (row \ "endtime").asOpt[String].getOrElse("0")
//    val pre_page = (row \ "pre_page").asOpt[String].getOrElse("")
//    val uid = (row \ "uid").asOpt[String].getOrElse("0")
//    val extend_params = (row \ "extend_params").asOpt[String].getOrElse("")
//    val app_name = (row \ "app_name").asOpt[String].getOrElse("")
//    val app_version = (row \ "app_version").asOpt[String].getOrElse("")
//    //    val os_version = (row \ "os_version").asOpt[String].getOrElse("")
//    val os = (row \ "os").asOpt[String].getOrElse("")
//    val utm = (row \ "utm").asOpt[String].getOrElse("0")
//    val source = (row \ "source").asOpt[String].getOrElse("")
//    //    val starttime_origin = (row \ "starttime_origin").asOpt[String].getOrElse("")
//    //    val endtime_origin = (row \ "endtime_origin").asOpt[String].getOrElse("")
//    val pre_extend_params = (row \ "pre_extend_params").asOpt[String].getOrElse("")
//    val url = (row \ "wap_url").asOpt[String].getOrElse("")
//    val urlref = (row \ "wap_pre_url").asOpt[String].getOrElse("")
//    val deviceid = (row \ "deviceid").asOpt[String].getOrElse("")
//    val jpid = (row \ "jpid").asOpt[String].getOrElse("")
//    val ip = (row \ "ip").asOpt[String].getOrElse("")
//    val to_switch = (row \ "to_switch").asOpt[String].getOrElse("0")
//    val location = (row \ "location").asOpt[String].getOrElse("")
//    val ctag = (row \ "c_label").asOpt[String].getOrElse("")
//    val server_jsonstr = (row \ "server_jsonstr").asOpt[String].getOrElse("")
//
//    val site_id = pageAndEventParser.getSiteId(app_name)
//    val ref_site_id = site_id
//    val gu_id = pageAndEventParser.getGuid(jpid, deviceid, os)
//    val terminal_id = pageAndEventParser.getTerminalId(os)

//    val pid = new GetPageID().evaluate("https://m.juanpi.com/act/sub_coudanguanyr11")
    val pid = 10069
    val res = pid.toInt match {
      case 34|65 => 3
      case 10069 => 4
      case _ => 0
    }
    println(res)
  }
}
