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
    testcid("cid:-1")
  }
}
