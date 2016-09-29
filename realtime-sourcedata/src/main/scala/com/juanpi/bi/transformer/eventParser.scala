package com.juanpi.bi.transformer

import com.juanpi.bi.hiveUDF.{GetGoodsId, GetPageID}

/**
  * Created by gongzi on 2016/9/28.
  */
object eventParser {

  def getForPageId(cid: String, f_page_extend_params: String, pagename: String): String = {
    val for_pageid = if("-1".equals(cid)) {
      "page_taball"
    } else if ("-2".equals(cid)) {
      "page_tabpast_zhe"
    } else if ("-3".equals(cid)) {
      "page_tabcrazy_zhe"
    } else if ("-4".equals(cid)) {
      "page_tabjiu"
    } else if ("-5".equals(cid) || "-6".equals(cid)) {
      "page_tabyugao"
    } else if ((!cid.isEmpty && cid.toInt > 0) | (cid == "-100" && (f_page_extend_params == "10045" || f_page_extend_params == "100105"))) {
      // when cast(get_json_object(server_jsonstr, '$.cid') as int) > 0 or (cast(get_json_object(server_jsonstr, '$.cid') as int) = -100 and page_extends_param in (10045,100105)) then 'page_tab'
      "page_tab"
    } else if ((cid == "0") && List("all", "past_zhe", "crazy_zhe", "jiu", "yugao").contains(f_page_extend_params)) {
      ""
    } else if ("page_h5".equals(pagename)) {
      val pid = new GetPageID().evaluate(f_page_extend_params).toInt
      pid match {
        case 34 | 65 | 10069 => "page_active"
        case _ => (pagename + f_page_extend_params).toLowerCase()
      }
    } else if (!"page_tab".equals(pagename)) {
      pagename
    } else {
      (pagename + f_page_extend_params).toLowerCase()
    }
    for_pageid
  }


  def getForExtendParams(activityname: String, t_extend_params: String, cube_position: String, server_jsonstr: String): String = {
    val f_extend_params =
      if ("click_cube_banner".equals(activityname)) {
        if (t_extend_params.contains("ads_id")) {
          val ads_id = pageAndEventParser.getJsonValueByKey(t_extend_params, "ads_id")
          "banner" + "::" + ads_id + cube_position
        } else {
          "banner" + "::" + t_extend_params + "::" + cube_position
        }
      }
      else if ("click_cube_block".equals(activityname) && !"{}".equals(server_jsonstr)) {
        // 有部分 click_cube_block 的数据格式错误，cube_position 与 server_jsonstr 的值传递反了
        server_jsonstr
      }
      else if (server_jsonstr.contains("pit_info")) {
        pageAndEventParser.getJsonValueByKey(server_jsonstr, "pit_info")
      } else if (t_extend_params.contains("pit_info")) {
        pageAndEventParser.getJsonValueByKey(t_extend_params, "pit_info")
      } else {
        t_extend_params
      }
    f_extend_params
  }

  def getExtendParamsFromBase(activityname: String, extend_params: String, app_version: String): String = {
    // 老版本 3.2.3
    val app_version323 = 323
    activityname match {
      case "click_temai_inpage_qq" => new GetGoodsId().evaluate(extend_params)
      case "click_temai_returngoods" => new GetGoodsId().evaluate(extend_params)
      case "click_temai_inpage_share" => new GetGoodsId().evaluate(extend_params)
      case "click_temai_inpage_collect" => new GetGoodsId().evaluate(extend_params)
      case "click_temai_inpage_cancelcollect" => new GetGoodsId().evaluate(extend_params)
      case "click_temai_orderdetails_complex" => new GetGoodsId().evaluate(extend_params)
      case "click_goods_tb" => new GetGoodsId().evaluate(extend_params)
      case "click_goods_cancel" => new GetGoodsId().evaluate(extend_params)
      case "click_goods_collection" => new GetGoodsId().evaluate(extend_params)
      case "click_goods_share" => new GetGoodsId().evaluate(extend_params)
      case a if "click_temai_inpage_joinbag".equals(activityname.toLowerCase()) && getVersionNum(app_version) < app_version323 => new GetGoodsId().evaluate(extend_params)
      case _ => extend_params.toLowerCase()
    }
  }

  def getEventId(d_event_id: Int, app_version: String): Int = {
    val app_ver = getVersionNum(app_version)
    val eid = d_event_id match {
      case a if (d_event_id == 0) => -1
      case b if (app_ver > 323 || d_event_id != 279) => d_event_id
      case _ => -999
    }
    eid
  }

  def getVersionNum(app_version: String): Int = {
    // TODO
    val resInt = if (app_version.nonEmpty) {
      app_version.replace(".", "").toInt
    }
    else {
      0
    }
    resInt
  }
}
