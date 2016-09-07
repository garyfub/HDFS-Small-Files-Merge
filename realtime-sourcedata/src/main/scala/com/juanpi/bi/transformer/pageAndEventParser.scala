package com.juanpi.bi.transformer

import java.util.regex.Pattern

import com.fasterxml.jackson.core.JsonParseException
import com.juanpi.bi.hiveUDF.{GetShopId, _}
import play.api.libs.json.{JsNull, JsValue, Json}

import scala.collection.mutable

/**
  * Created by gongzi on 2016/7/19.
  */
object pageAndEventParser {

  /**
    *
    * @param os
    * @return
    */
  def getTerminalId(os: String): Int = {
    os.toLowerCase match {
      case "android" => 2
      case "ios" => 3
      case _ => -999
    }
  }

  /**
    *
    * @param app_name
    * @return
    */
  def getSiteId(app_name: String): Int = {
    app_name.toLowerCase match {
      case "jiu" => 2
      case "zhe" => 1
      case _ => -999
    }
  }

  /**
    * 2016-02-16 加逻辑：IOS直接使用设备号作为gu_id
    *
    * @param jpid
    * @param deviceid
    * @param os
    * @return
    */
  def getGuid(jpid: String, deviceid: String, os: String): String = {
    if(jpid.equals("0") || jpid.isEmpty() ) {
      deviceid
    }
    else jpid
  }

  /**
    *
    * @param jsonStr
    * @param key
    * @return
    */
  def getJsonValueByKey(jsonStr: String, key: String): String = {
    if(jsonStr.contains(key)) {
      val js = Json.parse(jsonStr)
      (js \ key).toString()
    } else {
      ""
    }
  }

  /**
    *
    * @param jsonStr
    * @return
    */
  def getParsedJson(jsonStr: String): JsValue = {
    val v = if (jsonStr.nonEmpty && jsonStr.startsWith("{")) {
      try {
        Json.parse(jsonStr)
      } catch {
        //使用模式匹配来处理异常
        case ex: JsonParseException => {
          println(ex.getStackTraceString, "\n======>>异常的json数据: " + jsonStr)
        }
          JsNull
      }
    }
    else {
      JsNull
    }
    v
  }

  def getGsortPit(server_jsonstr: String): (Int, String) = {
    val js_server_jsonstr = getParsedJson(server_jsonstr)
    if (!js_server_jsonstr.equals(JsNull) && !server_jsonstr.equals("{}")) {
      val pit_type = (js_server_jsonstr \ "_pit_type").asOpt[Int].getOrElse(0)
      val gsort_key = (js_server_jsonstr \ "_gsort_key").asOpt[String].getOrElse("")
      (pit_type, gsort_key)
    } else {
      (0, "")
    }
  }

  /**
    *
    * @param gsort_key
    * @return
    */
  def getGsortKey(gsort_key: String): (String, String, String, String) = {
    if(gsort_key.nonEmpty && gsort_key.contains("_")) {
      val sortdate = Array(gsort_key.split("_")(3).substring(0, 4),gsort_key.split("_")(3).substring(4, 6),gsort_key.split("_")(3).substring(6, 8)).mkString("-")
      val sorthour = gsort_key.split("_")(4)
      val lplid = gsort_key.split("_")(5)
      var ptplid = ""
      if(gsort_key.split("_").length > 6 ){
        ptplid = gsort_key.split("_")(6)
      }
      (sortdate, sorthour, lplid, ptplid)
    }
    else ("", "", "", "")
  }

  /**
    *
    * @param pagename
    * @param extend_params
    * @param server_jsonstr
    * @return
    */
  def forPageId(pagename: String, extend_params: String, server_jsonstr: String): String = {
    val strValue = getParsedJson(server_jsonstr)
    val for_pageid = pagename.toLowerCase() match {
      case a if pagename.toLowerCase() == "page_tab" && isInteger(extend_params) && (extend_params.toLong > 0 && extend_params.toLong < 9999999) => "page_tab"
      case c if pagename.toLowerCase() == "page_tab" && !strValue.equals(JsNull) && (strValue \ "cid").asOpt[Int].getOrElse(0) < 0 => (pagename+(strValue \ "cid").asOpt[String]).toLowerCase()
      case b if pagename.toLowerCase() != "page_tab" => pagename.toLowerCase()
      case _ => (pagename+extend_params).toLowerCase()
    }
    for_pageid
  }

  /**
    * for page and event
    *
    * @param pagename
    * @param extend_params
    * @return
    */
  def getExtendParams(pagename: String, extend_params: String): String = {
    val extend_params_1 = pagename.toLowerCase() match {
      case "page_goods" | "page_temai_goods" | "page_temai_imagetxtgoods" | "page_temai_goods_logistics" | "page_peerpay_apply" => {
        new GetGoodsId().evaluate(extend_params)
      }
      case _ => {
        extend_params.toLowerCase()
      }
    }
    extend_params_1
  }

  /**
    * 二级页面值(品牌页：引流款ID等)
    *
    * @param x_page_id
    * @param x_extend_params
    * @param server_jsonstr
    * @return
    */
  def getPageLvl2Value(x_page_id: Int, x_extend_params: String, server_jsonstr: String): String = {
    val strValue = getParsedJson(server_jsonstr)
    val page_lel2_value =
      if(x_page_id == 250 && x_extend_params.nonEmpty
        && x_extend_params.contains("_")
        && x_extend_params.split("_").length > 2)
      {
        new GetGoodsId().evaluate(x_extend_params.split("_")(2))
      }
      else if(x_page_id == 154 || x_page_id == 289) {
        val pid = new GetPageID().evaluate(x_extend_params)
        if(pid == 10104) {
          new GetSkcId().evaluate(x_extend_params)
        }
        else if(pid == 10102) {
          new GetShopId().evaluate(x_extend_params)
        } else ""
      }
      else if(x_page_id == 169 && !strValue.equals(JsNull) && server_jsonstr.contains("order_status")) {
        (strValue \ "order_status").toString()
      }
      else ""
    page_lel2_value
  }


  /**
    *
    * @param page_id
    * @param extend_params
    * @param page_level_id
    * @return
    */
  def getPageLevelId(page_id: Int, extend_params: String, page_level_id: Int): Int = {
    if(page_id != 154 || page_id != 289)
      {
        page_level_id
      } else {
      val pid = new GetPageID().evaluate(extend_params)
      pid.toInt match {
        case 34|65 => 2
        case 10069 => 3
        case _ => 0
      }
    }
  }

  /**
    *
    * @param x_page_id
    * @param extend_params, 正确的格式应该有两种：brandid_shopid_hotgoodsid, brandid_shopid。错误的格式：为空或者只有 brandid
    * @return
    */
  def getShopId(x_page_id: Int, extend_params: String): String = {
    val shop_id = if(x_page_id == 250 && extend_params.contains("_")){
      new GetGoodsId().evaluate(extend_params.split("_")(1))
    }
    else {
      0
    }
    shop_id.toString()
  }

  /**
    *
    * @param x_page_id
    * @param x_extend_params
    * @return
    */
  def getPageId(x_page_id: Int, x_extend_params: String): Int = {
    if(x_page_id == 0) {
      -1
    } else if (x_page_id == 289 || x_page_id == 154) {
      val pid = new GetPageID().evaluate(x_extend_params)
      if(pid > 0) {
        pid
      } else {
        x_page_id
      }
    } else {
      x_page_id
    }
  }

  /**
    *
    * @param x_page_id
    * @param x_extend_params
    * @param page_type_id
    * @param x_page_value
    * @return
    */
  def getPageValue(x_page_id:Int, x_extend_params: String, page_type_id: Int, x_page_value: String): String = {
    // 解析 page_value
    val page_value: String =
      if (x_page_id == 289 || x_page_id == 154) {
        new GetDwPcPageValue().evaluate(x_extend_params)
      } else {
        if(x_page_id == 254) {
          new GetDwMbPageValue().evaluate(x_extend_params.toString, page_type_id.toString)
        } else if(page_type_id == 1 || page_type_id == 4 || page_type_id == 10) {
          new GetDwMbPageValue().evaluate(x_page_value, page_type_id.toString)
        }
        else if(x_page_id == 250) {
          // by gognzi on 2016-04-24 17:10
          // app端品牌页面id = 250,extend_params格式：加密brandid_shopid_引流款id,或者 加密brandid_shopid
          // getgoodsid(split(a.extend_params,'_')[0])
          new GetDwMbPageValue().evaluate(new GetGoodsId().evaluate(x_extend_params.split("_")(0)), page_type_id.toString)
        } else {
          new GetDwMbPageValue().evaluate(x_extend_params, page_type_id.toString)
        }
      }
    page_value
  }

  // http://stackoverflow.com/questions/9028459/a-clean-way-to-combine-two-tuples-into-a-new-larger-tuple-in-scala
  def combineTuple(xss: Product*) = xss.toList.flatten(_.productIterator)

  /*
* 判断是否为整数
* @param str 传入的字符串
* @return 是整数返回true,否则返回false
*/
  def  isInteger(str: String): Boolean = {
    val pattern: Pattern = Pattern.compile("^[-\\+]?[\\d]*$")
    if (str.nonEmpty) {
      pattern.matcher(str).matches()
    } else {
      false
    }
  }

  /**
    * 只在pageinfo中有
    *
    * @param source
    * @return
    */
  def getSource(source: String): String = {
    val s = source match {
      case a if a.isEmpty() | a == "null" | !a.contains("push") => "未知"
      case b if b.contains("订单") => "用户个人订单信息推送"
      case c if c.contains("售后", "退货", "退款") => "用户售后信息推送"
      case d if d.contains("你好") => "用户个人消息通知推送"
      case e if e.contains("有货就赶紧抢") => "有货提醒"
      case f if f.contains("收藏的商品") => "用户收藏商品最新消息推送"
      case g if g.contains("订单") => "用户个人订单信息推送"
      case _ => source.substring(6)
    }
    s
  }

  def main(args: Array[String]) {
//    println(getSource("push:小卷温馨提醒！=购物车的商品等你好久啦！你爱的时尚亲肤卡通床品套件低至49.00元疯抢中，果断带宝贝回家→=3465969792363098765"))

    val dimPages_test = new mutable.HashMap[String, (Int, Int, String, Int)]
    dimPages_test += ("page_taball" -> (219,10,"最新折扣",1))
    val (d_page_id: Int, page_type_id: Int, d_page_value: String, d_page_level_id: Int) = dimPages_test.get("page_taball").getOrElse(0, 0, "", 0)
//    println(d_page_id, page_type_id, d_page_value,d_page_level_id)

    val s = """"server_jsonstr":"{\"ads_id\":\"1928\",\"user_group_id\":\"\"}""""
    val sr = getParsedJson(s)
    println(sr)

    if(getParsedJson(s).equals(JsNull)) println("test")

    val extend_params = ""

    println(getGsortPit(s))

    println(getJsonValueByKey(s, "item"))

    println(getJsonValueByKey(s, "_rmd"))

    println(getJsonValueByKey(s, "_t"))

    val ab_info = pageAndEventParser.getJsonValueByKey(extend_params, "ab_info")
    println(ab_info)
    println(pageAndEventParser.getJsonValueByKey(ab_info, "rule_id"))

    val strValue = getParsedJson(extend_params)
    println((strValue \ "cid").asOpt[Int].getOrElse(0))

    if(!strValue.equals(JsNull) && s.contains("order_status")) {
      val res = (strValue \ "order_status").toString()
      println("res:==" + res)
    }

  }


}
