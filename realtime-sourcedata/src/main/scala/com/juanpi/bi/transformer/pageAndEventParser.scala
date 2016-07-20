package com.juanpi.bi.transformer

import java.util.regex.Pattern

import com.juanpi.bi.hiveUDF.{GetDwMbPageValue, GetDwPcPageValue, GetGoodsId, GetPageID}
import play.api.libs.json.Json

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
  def getTerminalId(os: String): Int =
  {
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
  def getSiteId(app_name: String): Int =
  {
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
  def getGuid(jpid: String, deviceid: String, os: String): String =
  {
    if(jpid.equals("0") || jpid.isEmpty() )
    {
      deviceid
    }
    else jpid
  }

  /**
    *
    * @param pagename
    * @param extend_params
    * @param server_jsonstr
    * @return
    */
  def forPageId(pagename: String, extend_params: String, server_jsonstr: String): String =
  {

    val for_pageid = pagename.toLowerCase() match
    {
      case a if pagename.toLowerCase() == "page_tab" && isInteger(extend_params) && (extend_params.toInt > 0 && extend_params.toInt < 9999999) => "page_tab"
      case c if pagename.toLowerCase() == "page_tab" && !server_jsonstr.isEmpty() && (Json.parse(server_jsonstr) \ "cid").asOpt[Int].getOrElse(0) < 0 => (pagename+(Json.parse(server_jsonstr) \ "cid").asOpt[String]).toLowerCase()
      case b if pagename.toLowerCase() != "page_tab" => pagename.toLowerCase()
      case _ => (pagename+extend_params).toLowerCase()
    }
    for_pageid
  }

  /**
    * for page and event
    * @param pagename
    * @param extend_params
    * @return
    */
  def getExtendParams(pagename: String, extend_params: String): String =
  {
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
    *
    * @param x_page_id
    * @param x_extend_params
    * @return
    */
  def getPageId(x_page_id: Int, x_extend_params: String): Int =
  {
    if(x_page_id == 0)
    {
      -1
    }else if (x_page_id == 289 || x_page_id == 154)
    {
      val pid = new GetPageID().evaluate(x_extend_params)
      if(pid > 0)
      {
        pid
      }
      else x_page_id
    }
    else
      x_page_id
  }

  /**
    *
    * @param x_page_id
    * @param x_extend_params
    * @param page_type_id
    * @param x_page_value
    * @return
    */
  def getPageValue(x_page_id:Int, x_extend_params: String, page_type_id: Int, x_page_value: String): String =
  {
    // 解析 page_value
    val page_value: String =
      if (x_page_id == 289 || x_page_id == 154)
      {
        new GetDwPcPageValue().evaluate(x_extend_params)
      }
      else
      {
        if(x_page_id == 254)
        {
          new GetDwMbPageValue().evaluate(x_extend_params.toString, page_type_id.toString)
        }
        else if(page_type_id == 1 || page_type_id == 4 || page_type_id == 10)
        {
          new GetDwMbPageValue().evaluate(x_page_value, page_type_id.toString)
        }
        else if(x_page_id == 250)
        {
          // by gognzi on 2016-04-24 17:10
          // app端品牌页面id = 250,extend_params格式：加密brandid_shopid_引流款id,或者 加密brandid_shopid
          // getgoodsid(split(a.extend_params,'_')[0])
          new GetDwMbPageValue().evaluate(new GetGoodsId().evaluate(x_extend_params.split("_")(0)), page_type_id.toString)
        }
        else
        {
          new GetDwMbPageValue().evaluate(x_extend_params, page_type_id.toString)
        }
      }
    page_value
  }

  /*
* 判断是否为整数
* @param str 传入的字符串
* @return 是整数返回true,否则返回false
*/
  def  isInteger(str: String): Boolean =
  {
    val pattern: Pattern = Pattern.compile("^[-\\+]?[\\d]*$")
    //    pattern.matcher(str).matches()
    if (!str.isEmpty()) pattern.matcher(str).matches() else false
  }

  /**
    * 只在pageinfo中有
    * @param source
    * @return
    */
  def getSource(source: String): String =
  {
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
    println(getSource("push:小卷温馨提醒！=购物车的商品等你好久啦！你爱的时尚亲肤卡通床品套件低至49.00元疯抢中，果断带宝贝回家→=3465969792363098765"))

    val dimPages_test = new mutable.HashMap[String, (Int, Int, String, Int)]
    dimPages_test += ("page_taball" -> (219,10,"最新折扣",1))
    val (d_page_id: Int, page_type_id: Int, d_page_value: String, d_page_level_id: Int) = dimPages_test.get("page_taball").getOrElse(0, 0, "", 0)
    println(d_page_id, page_type_id, d_page_value,d_page_level_id)
  }


}