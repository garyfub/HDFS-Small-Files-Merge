package com.juanpi.bi.transformer

import play.api.libs.json.JsNull

/**
  * Created by gongzi on 2016/9/30.
  */
object pageParser {
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
