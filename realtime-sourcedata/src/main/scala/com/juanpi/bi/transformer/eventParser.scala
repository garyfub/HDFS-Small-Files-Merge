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

  // outlierData 离群数据过滤
  /**
    * 如果 page_name="page_tab",并且 cid 为空，且f_page_extend_params不在("all", "past_zhe", "crazy_zhe", "jiu", "yugao")之中，就过滤掉
    * 如果 pageName = "page_h5" 且 pid = -1
    * @param pageName
    * @param cid
    * @param fctPageExtendParams
    * @return
    */
  def filterOutlierPageId(pageName: String, cid: String, fctPageExtendParams: String): Boolean = {
    val flag = if("page_tab".equals(pageName)
      && cid.isEmpty
      && !List("all", "past_zhe", "crazy_zhe", "jiu", "yugao").contains(fctPageExtendParams)) {
      true
    } else if("page_h5".equals(pageName)) {
      val pid = new GetPageID().evaluate(fctPageExtendParams).toInt
      pid match {
        case -1 => true
        case _ => false
      }
    } else false
    flag
  }

  def getForPrePageId(pagename: String, f_pre_extend_params: String, pre_page: String):String = {
    val forPrePageId =
      if ("page_h5".equals(pagename)) {
        val pid = new GetPageID().evaluate(f_pre_extend_params).toInt
        pid match {
          case 34 | 65 | 10069 => "page_active"
          case _ => (pagename + f_pre_extend_params).toLowerCase()
        }
      } else if (!"page_tab".equals(pre_page)) {
        pre_page.toLowerCase()
      }
      else {
        (pre_page + f_pre_extend_params).toLowerCase()
      }
    forPrePageId
  }

  def getForEventId(cid: String, activityname: String, t_extend_params: String): String = {
    val forEventId = if("-6".equals(cid)) {
      "click_yugao_recommendation"
    } else if("-100".equals(cid)) {
      "click_shoppingbag_recommendation"
    } else if("-101".equals(cid)) {
      "click_orderdetails_recommendation"
    } else if ("-102".equals(cid)) {
      "click_detail_recommendation"
    } else if (!"click_navigation".equalsIgnoreCase(activityname)) {
      activityname
    } else {
      (activityname + t_extend_params).toLowerCase()
    }
    forEventId
  }

  def getForExtendParams(activityname: String, t_extend_params: String, cube_position: String, server_jsonstr: String): String = {
    val f_extend_params =
      if ("click_cube_banner".equals(activityname)) {
        if (t_extend_params.contains("ads_id")) {
          val ads_id = pageAndEventParser.getJsonValueByKey(t_extend_params, "ads_id")
          "banner" + "::" + ads_id + "::" + cube_position
        } else {
          "banner" + "::" + t_extend_params + "::" + cube_position
        }
      }
      else if (server_jsonstr.contains("pit_info")) {
        pageAndEventParser.getJsonValueByKey(server_jsonstr, "pit_info")
      } else if (t_extend_params.contains("pit_info")) {
        pageAndEventParser.getJsonValueByKey(t_extend_params, "pit_info")
      }
      else if ("click_cube_block".equals(activityname) && !"{}".equals(server_jsonstr)) {
        // 有部分 click_cube_block 的数据格式错误，cube_position 与 server_jsonstr 的值传递反了
        server_jsonstr
      } else if (server_jsonstr.contains("pit_info")) {
        pageAndEventParser.getJsonValueByKey(server_jsonstr, "pit_info")
      } else if (t_extend_params.contains("pit_info")) {
        pageAndEventParser.getJsonValueByKey(t_extend_params, "pit_info")
      } else {
        t_extend_params
      }
    f_extend_params
  }

  /**
    * 从base层解析 extendparams
    * @param activityname
    * @param extend_params
    * @param app_version
    * @return
    */
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

  def getEventValue(event_type_id: Int, activityname: String, extend_params: String, server_jsonstr: String): String =
  {
    val operTime = pageAndEventParser.getJsonValueByKey(server_jsonstr, "_t")

    if (event_type_id == 10) {
      if (activityname.contains("click_cube")) {
        extend_params
      } else if (!server_jsonstr.contains("_t") || operTime.isEmpty) {
        ""
      } else {
        extend_params
      }
    } else {
      extend_params
    }
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
