package com.juanpi.bi.transformer

import com.juanpi.hive.udf.{GetGoodsId, GetPageID, GetShopId, GetSkcId}
import play.api.libs.json.JsNull

/**
  * Created by gongzi on 2016/9/30.
  */
object pageParser {


  /**
    * 二级页面值(品牌页：引流款ID；商品skc、分享回流标识)
    * @param x_page_id
    * @param x_extend_params
    * @param server_jsonstr
    * @param url
    * @return
    */
  def getPageLvl2Value(x_page_id: Int, x_extend_params: String, server_jsonstr: String, url: String): String = {
    val strValue = pageAndEventParser.getParsedJson(server_jsonstr)
    val page_lel2_value =
      if(x_page_id == 250 && x_extend_params.nonEmpty
        && x_extend_params.contains("_")
        && x_extend_params.split("_").length > 2)
      {
        // 解析品牌页的引流款商品
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
      else if(x_page_id == 169
        && !strValue.equals(JsNull)
        && server_jsonstr.contains("order_status")) {
        (strValue \ "order_status").toString()
      } else if(url.nonEmpty) {
        url match {
          case a if a.contains("singlemessage") => "singlemessage"
          case b if b.contains("groupmessage") => "groupmessage"
          case c if c.contains("timeline") => "timeline"
          case _ => ""
        }
      }
      else {""}
    page_lel2_value
  }


  /**
    *
    * @param pagename
    * @param extend_params
    * @param server_jsonstr
    * @return
    */
  def forPageId(pagename: String, extend_params: String, server_jsonstr: String): String = {
    val strValue = pageAndEventParser.getParsedJson(server_jsonstr)
    val for_pageid = pagename.toLowerCase() match {
      case a if pagename.toLowerCase() == "page_tab"
        && pageAndEventParser.isInteger(extend_params)
        && (extend_params.toLong > 0
        && extend_params.toLong < 9999999) => "page_tab"
      case c if pagename.toLowerCase() == "page_tab"
        && !strValue.equals(JsNull)
        && (strValue \ "cid").asOpt[Int].getOrElse(0) < 0
      => (pagename+(strValue \ "cid").asOpt[String]).toLowerCase()
      case b if pagename.toLowerCase() != "page_tab" => pagename.toLowerCase()
      case _ => (pagename+extend_params).toLowerCase()
    }
    for_pageid
  }
}
