package com.juanpi.bi.transformer

import java.util.regex.{Matcher, Pattern}

import com.juanpi.hive.udf.{GetDwMbPageValue, GetDwPcPageValue, GetGoodsId, GetPageID}
import play.api.libs.json.{JsNull, Json}

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
      val pid = new GetPageID().evaluate(f_page_extend_params)
      val pageId = if(pid == null) {0} else pid.toInt
      if (pageId > 0) { "page_active" } else (pagename + f_page_extend_params).toLowerCase()
    } else if (!"page_tab".equals(pagename)) {
      pagename
    } else {
      (pagename + f_page_extend_params).toLowerCase()
    }
    for_pageid
  }

  /**
    *
    * @param x_page_id
    * @param x_extends_param
    * @return
    */
  def getPageLvl2Value(x_page_id: Int, x_extends_param: String): String = {
    val page_lel2_value =
      if(x_page_id == 250 && x_extends_param.nonEmpty
        && x_extends_param.contains("_")
        && x_extends_param.split("_").length > 2)
      {
        // 解析品牌页的引流款商品
        new GetGoodsId().evaluate(x_extends_param.split("_")(2))
      }
     else {""}
    page_lel2_value
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
  def filterOutlierPageId(activityName: String, pageName: String, cid: String, fctPageExtendParams: String): Boolean = {
    val flag = if(pageName.isEmpty || activityName.isEmpty) {
      true
    }
    else if(activityName.contains("exposure_") || activityName.contains("_performance") || "collect_page_h5".equals(activityName)){
      // 过滤event中的曝光数据：exposure_ad_popup, exposure_ad_inscreen
      // 和性能采集数据collect_data_performance, collect_page_performance
      true
    }
    else if("page_tab".equals(pageName)
      && cid.isEmpty
      && !List("all", "past_zhe", "crazy_zhe", "jiu", "yugao").contains(fctPageExtendParams)) {
      true
    } else if("page_h5".equals(pageName)) {
      val pid = new GetPageID().evaluate(fctPageExtendParams)
      val pageId = if(pid == null) {0} else pid.toInt
      pageId match {
        case -1 => true
        case _ => false
      }
    } else false
    flag
  }

  def getForPrePageId(pagename: String, f_pre_extend_params: String, pre_page: String):String = {
    val forPrePageId =
      if ("page_h5".equals(pagename)) {
        val pid = new GetPageID().evaluate(f_pre_extend_params)
        val pageId = if(pid == null) {0} else pid.toInt
        if (pageId > 0) {
          "page_active"
        } else {
          (pagename + f_pre_extend_params).toLowerCase()
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
      (activityname + t_extend_params).toLowerCase
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
      case a if "click_temai_inpage_joinbag".equals(activityname) && getVersionNum(app_version) < app_version323 => new GetGoodsId().evaluate(extend_params)
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

  /**
    *
    * @param x_page_id
    * @param x_extend_params
    * @param page_type_id
    * @param x_page_value
    * @return
    */
  def getPageValue(x_page_id:Int, x_extend_params: String, cid: String, page_type_id: Int, x_page_value: String): String = {
    // 解析 page_value
    val page_value: String =
    if (x_page_id == 289 || x_page_id == 154) {
      val res = new GetDwPcPageValue().evaluate(x_extend_params)
      res
    } else {
      val param = if(x_page_id == 254)
      {
        if("10045".equals(x_extend_params) || "100105".equals(x_extend_params)) {
          x_extend_params
        } else {
          cid
        }
      } else if(page_type_id == 1 || page_type_id == 4 || page_type_id == 10) {
        x_page_value
      } else if(x_page_id == 250) {
        // app端品牌页面id = 250, page_extends_param 格式：加密brandid_shopid_引流款id,或者 加密brandid_shopid
        val goodsId = new GetGoodsId().evaluate(x_extend_params.split("_")(0))
        goodsId
      } else {
        x_extend_params
      }
      val res = new GetDwMbPageValue().evaluate(param, page_type_id.toString)
      res
    }
    page_value
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

  /**
    * ab测试，选择A还是B
    * @param server_jsonstr
    * @return
    */
  def getAbinfo(server_jsonstr: String): (String, String) = {
    var selectId:String = ""
    var testId:String = ""
    if (server_jsonstr.contains("ab_info")) {
      val ab_info = pageAndEventParser.getJsonValueByKey(server_jsonstr, "ab_info")
      val pat = Pattern.compile("([A-Z])([0-9]\\d*)")
      val mch = pat.matcher(ab_info)
      if(mch.find()){
        selectId = mch.group(1)
        testId = mch.group(2)
      } else {
        selectId = pageAndEventParser.getJsonValueByKey(ab_info, "select")
        testId = pageAndEventParser.getJsonValueByKey(ab_info, "test_id")
      }
      (selectId, testId)
    } else ("", "")
  }

  /**
    *
    * @param gsort_key
    * @return
    */
  def getGsortKey(gsort_key: String): (String, String, String, String) = {
    val defaultPat = Pattern.compile("_SORT_(-?[0-9]\\d*)_(-?[0-9]\\d*)_(-?[0-9]\\d*)_(-?[0-9]\\d*)")
    val positionPat = Pattern.compile("POSTION_SORT_(-?[0-9]\\d*)_(-?[0-9]\\d*)_(-?[0-9]\\d*)_(-?[0-9]\\d*)_(-?[0-9]\\d*)")
    val mch: Matcher = defaultPat.matcher(gsort_key)
    if(mch.find()) {
      val sortdate = mch.group(2)
      val sorthour = mch.group(3)
      val lplid = mch.group(4)
      val pMch: Matcher = positionPat.matcher(gsort_key)
      val ptplid: String = if(pMch.find()) {
        pMch.group(3)
      }
      else ""
      (sortdate, sorthour, lplid, ptplid)
    }
    else ("", "", "", "")
  }

  /**
    * 过滤函数，满足条件的留下，不满足的过滤掉
    * @param line
    * @return
    */
  def filterFunc(line: String): Boolean = {
    val row = Json.parse(line)
    val activityName = (row \ "activityname").asOpt[String].getOrElse("").toLowerCase()
    val blackArray = Array("exposure_temai_pic", "collect_mainpage_loadtime", "exposure_ad_welt", "collect_popup_unlock", "crash_exception_info", "exposure_ad_inscreen", "exposure_ad_popup_sec", "exposure_ad_popup", "show_temai_pay_applepay", "collect_api_responsetime", "collect_page_h5", "collect_data_performance", "collect_page_performance")
    val isKeep =
      if(activityName.contains("_performance")) {
        false
      } else {
        // 包含上述
        !blackArray.exists(_ == activityName)
      }
    // 满足条件的留下，不满足的过滤掉
    isKeep
  }


  /**
    *
    * @param server_jsonstr
    * @return
    */
  def getGsortPit(server_jsonstr: String): (Int, String) = {
    val js_server_jsonstr = pageAndEventParser.getParsedJson(server_jsonstr)
    if (!js_server_jsonstr.equals(JsNull) && !server_jsonstr.equals("{}")) {
      val pit_type = (js_server_jsonstr \ "_pit_type").asOpt[Int].getOrElse(0)
      val gsort_key = (js_server_jsonstr \ "_gsort_key").asOpt[String].getOrElse("")
      (pit_type, gsort_key)
    } else {
      (0, "")
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
