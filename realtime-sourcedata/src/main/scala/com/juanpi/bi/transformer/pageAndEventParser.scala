package com.juanpi.bi.transformer

import java.util.regex.Pattern

import com.fasterxml.jackson.core.JsonParseException
import com.juanpi.bi.sc_utils.{DateUtils, StringUtils}
import com.juanpi.hive.udf.{GetDwMbPageValue, GetDwPcPageValue, GetGoodsId, GetPageID}
import play.api.libs.json._

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
    *
    * @param jpid
    * @param deviceid (ios)
    * @param os
    * @return
    */
  def getGuid(jpid: String, deviceid: String, os: String): String = {
    val devId = if(deviceid.equals("0") || deviceid.isEmpty()){
      ""
    }
    else {
      deviceid
    }

    val gu_id = if(jpid.equals("0") || jpid.isEmpty()) {
      devId
    }
    else { jpid }
    gu_id
  }

  /**
    * 根据开始时间分区，如果 startTimeOrigin 与 startTime 是同一天，就取 startTimeOrigin；如果startTimeOrigin为空，就取 startTime
    * @param startTimeOrigin
    * @param startTime
    * @return
    */
  def getPartitionTime(startTimeOrigin: String, startTime: String): String = {

    val originDate = if(startTimeOrigin.nonEmpty) {
      DateUtils.dateHourStr(startTimeOrigin.toLong)._1
    } else ""

    val date = if(startTime.nonEmpty) {
      DateUtils.dateHourStr(startTime.toLong)._1
    } else ""

    val res = if(date.equals(originDate)){
      startTimeOrigin
    } else if(startTimeOrigin.isEmpty) {
      startTime
    } else {
      startTime
    }
    res
  }

  /**
    * 由于原始数据同一个字段传值的类型不完全一样，比如cid，有时候传的是整形，有时候又是字符串。
    * 强化 getJsonValueByKey 函数，根据Json中解析到的类型进行判断，然后再转为String，
    * @param jsonStr
    * @param key
    * @return
    */
  def getJsonValueByKey(jsonStr: String, key: String): String = {
    if (jsonStr.contains(key)) {
      val js = Json.parse(jsonStr)
      val v = (js \ key)
      v match {
        case a if v.isInstanceOf[JsString] => v.asOpt[String].getOrElse("")
        case b if v.isInstanceOf[JsNumber] => v.toString()
        case c if v.isInstanceOf[JsObject] => v.toString()
        case _ => ""
      }
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
          println(ex.getStackTraceString)
          println("\n======>>异常的json数据: " + jsonStr)
        }
          JsNull
      }
    }
    else {
      JsNull
    }
    v
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
        val goodsId = new GetGoodsId().evaluate(extend_params)
        goodsId
      }
      case _ => {
        extend_params.toLowerCase()
      }
    }
    extend_params_1
  }

  /**
    *
    * @param x_page_id
    * @param x_extend_params
    * @param page_level_id
    * @return
    */
  def getPageLevelId(x_page_id: Int, x_extend_params: String, page_level_id: Int, forLevelId: String): Int = {
    if(x_page_id == 254 && forLevelId == "2") {
      StringUtils.strToInt(forLevelId)
    } else if(x_page_id != 154 || x_page_id != 289) {
      page_level_id
    } else {
      val pid = new GetPageID().evaluate(x_extend_params)
      val res = pid.toInt match {
        case 34|65 => 3
        case 10069 => 4
        case _ => 0
      }
      res
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
      try {
          new GetGoodsId().evaluate(extend_params.split("_")(1))
        }catch {
        //使用模式匹配来处理异常
        case ex: Exception => println(ex.printStackTrace() + "==>>getShopId======>>异常数据:" + extend_params)
        0
        }
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
      // 如果 x_extend_params 为空，pid的计算结果为null
      val pid = new GetPageID().evaluate(x_extend_params)
      if(pid == null){
        -1
      } else if(pid > 0) {
        pid
      } else {
        x_page_id
      }
    } else {
      x_page_id
    }
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

//    val s = """"server_jsonstr":"{\"ads_id\":\"1928\",\"user_group_id\":\"\"}""""
    val s = "{}"
    val sr = getParsedJson(s)
    println(sr)

    if(getParsedJson(s).equals(JsNull)) println("test")

    val extend_params = ""

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

      println("gu_id:" + getGuid("0", "0", ""))

    println("1_2_3".split("_").length)
    println("1_2_3".split("_").length)
  }


}
