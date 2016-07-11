//package com.juanpi.bi.streaming
//
//import com.juanpi.bi.utils.{GetGoodsId, GetMbPageId, ReadMysql}
//import play.api.libs.json.{JsValue, Json}
///**
//  * Created by gongzi on 2016/06/25.
//  */
//object ParsePageInfo {
//
//  // todo ticks_history 的结构，通过 hbase 来存储、更新
//  // todo 当page或者event中的gu_id在 ticks_history 没找到对应的数据，就写 hbase
//  // todo 当page或者event中的gu_id在 ticks_history 找到对应的数据，就从 hbase 读取
//  var ticks_history = scala.collection.mutable.Map("gu_id_app_name" -> Map("utm_id" -> 0, "gu_ctime" -> "2016-05-07"))
//
//  val parsePageInfo = (row: JsValue) =>
//  {
//    val ticks = (row \ "ticks").asOpt[String].getOrElse("")
//    val session_id = (row \ "session_id").asOpt[String].getOrElse("")
//    val pagename = (row \ "pagename").asOpt[String].getOrElse("")
//    val starttime = (row \ "starttime").asOpt[String].getOrElse("0")
//    val endtime = (row \ "endtime").asOpt[Long].getOrElse(0)
//    val pre_page = (row \ "pre_page").asOpt[String].getOrElse("")
//    val uid = (row \ "uid").asOpt[String].getOrElse("0")
//    val extend_params = (row \ "extend_params").asOpt[String].getOrElse("")
//    val app_name = (row \ "app_name").asOpt[String].getOrElse("")
//    val app_version = (row \ "app_version").asOpt[String].getOrElse("")
//    val os_version = (row \ "os_version").asOpt[String].getOrElse("")
//    val os = (row \ "os").asOpt[String].getOrElse("")
//    var utm = (row \ "utm").asOpt[String].getOrElse("0")
//    val source = (row \ "source").asOpt[String].getOrElse("")
//    val starttime_origin = (row \ "starttime_origin").asOpt[String].getOrElse("")
//    val endtime_origin = (row \ "endtime_origin").asOpt[String].getOrElse("")
//    val pre_extend_params = (row \ "pre_extend_params").asOpt[String].getOrElse("")
//    val wap_url = (row \ "wap_url").asOpt[String].getOrElse("")
//    val wap_pre_url = (row \ "wap_pre_url").asOpt[String].getOrElse("")
//    val deviceid = (row \ "deviceid").asOpt[String].getOrElse("")
//    val jpid = (row \ "jpid").asOpt[String].getOrElse("")
//    val ip = (row \ "ip").asOpt[String].getOrElse("")
//    val to_switch = (row \ "to_switch").asOpt[Int].getOrElse("0")
//    val location = (row \ "location").asOpt[String].getOrElse("")
//    val c_label = (row \ "c_label").asOpt[String].getOrElse("")
//    val server_jsonstr = (row \ "server_jsonstr").asOpt[String].getOrElse("")
//    val c_server = (row \ "c_server").asOpt[String].getOrElse("")
//
//    var gu_ctime = ""
//
//    val gu_id = os.toLowerCase match
//    {
//      case "android" => jpid
//      case "ios" => deviceid
//      case _ => "0"
//    }
//
//    // todo 查hbase 从 ticks_history 中查找 ticks 存在的记录
//    if(ticks_history.contains( ticks+app_name ))
//    {
//      utm = ticks_history.get(ticks+app_name).get("utm").toString
//      gu_ctime = ticks_history.get(ticks+app_name).get("gu_ctime").toString
//    }else{
//      println(ticks+app_name + " 键不存在!")
//    }
//
//    val extend_params_1 = pagename.toLowerCase() match {
//      case "page_goods" | "page_temai_goods" | "page_temai_imagetxtgoods" | "page_temai_parametergoods" => {
//        GetGoodsId.evaluate(extend_params)
//      }
//      case _ => {
//        extend_params.toLowerCase()
//      }
//    }
//
//    val pageId = GetMbPageId.evaluate(pagename.toLowerCase(), extend_params_1)
//    val goodsId = if ((pageId == 158) ||
//      (pageId == 159 && (app_version == "3.2.3" || app_version == "3.2.4") && (os.toLowerCase() == "ios"))) {
//      extend_params_1
//    } else {
//      "-1"
//    }
//
//    val logTime = if (starttime.size == 0) {
//      0L
//    } else {
//      starttime.toLong
//    }
//
//    val validGoodsId = try {
//      if (goodsId.size == 0) {
//        "-1"
//      } else {
//        val goods = goodsId.toInt
//        goods.toString
//      }
//    } catch {
//      case ex: NumberFormatException => {
//        println("======>> pageinfo解析异常" + ":" + goodsId + ":" + row)
//        "-1"
//      }
//    }
//
//    // gu_id
//    val id = if ((uid.length == 0) || uid.equals("0")) {
//      gu_id
//    } else {
//      uid
//    }
//
//    (validGoodsId, id, logTime)
//  }
//
//
//  def main(args: Array[String]) {
//    val liuliang = """{"session_id":"1448943287122_jiu_1457400715757","ticks":"1448943287122","uid":"29617028","utm":"102524","app_name":"jiu","app_version":"3.4.0","os":"android","os_version":"5.1","deviceid":"867905020977700","jpid":"ffffffff-e5c4-5627-25f1-ba271bc27f63","to_switch":"1","location":"湖南省","c_label":"C2","source":"","starttime":"1457400759215","endtime":"1457400761605","pagename":"page_h5","extend_params":"http://m.juanpi.com/help/customer","pre_page":"page_center","pre_extend_params":"","wap_url":"http://m.juanpi.com/help/customer","wap_pre_url":"about:blank","gj_page_names":"page_tab,page_tab","gj_ext_params":"all,all","starttime_origin":"1457400758719","endtime_origin":"1457400761109","ip":"111.8.228.232"}"""
//    val row = Json.parse(liuliang)
//    println(row \ ("session_id"))
//    println(parsePageInfo(row))
//  }
//}
