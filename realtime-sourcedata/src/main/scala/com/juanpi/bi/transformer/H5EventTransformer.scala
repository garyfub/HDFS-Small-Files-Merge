package com.juanpi.bi.transformer
import com.juanpi.bi.bean.{Event, Page, PageAndEvent, User}
import com.juanpi.bi.init.ScalaConstants
import com.juanpi.bi.sc_utils.DateUtils
import com.juanpi.hive.udf._
import play.api.libs.json.{JsValue, Json}

import scala.collection.mutable

/**
  * Created by gongzi on 2016/11/28.
  */
class H5EventTransformer {

  def logParser(line: String,
                dimPage: mutable.HashMap[String, (Int, Int, String, Int)],
                dimEvent: mutable.HashMap[String, (Int, Int)]
               ): (String, String, Any) = {

    val row = Json.parse(line)
    val ticks = (row \ "ticks").asOpt[String].getOrElse("")
    val jpid = (row \ "jpid").asOpt[String].getOrElse("")
    val deviceId = (row \ "deviceid").asOpt[String].getOrElse("")
    val os = (row \ "os").asOpt[String].getOrElse("")
    val endTime = (row \ "endtime").as[String].toLong

    // TODO 逻辑待优化
    if (ticks.length() >= 13) {
      // 解析逻辑
      var gu_id = ""
      try {
        gu_id = pageAndEventParser.getGuid(jpid, deviceId, os)
      } catch {
        // 使用模式匹配来处理异常
        case ex: Exception => {
          println(ex.getStackTraceString)
        }
          println("=======>> PcEvent: getGuid Exception!!" + "======>>异常数据:" + row)
      }

      val ret = if (gu_id.nonEmpty) {
        val endtime = (row \ "endtime").asOpt[String].getOrElse("")
        val server_jsonstr = (row \ "server_jsonstr").asOpt[String].getOrElse("")

        try {
          val res = parse(row, dimPage, dimEvent)
          // 过滤异常的数据，具体见解析函数 eventParser.filterOutlierPageId
          if (res == null) {
            ("", "", None)
          }
          else {
            val (user: User, pageAndEvent: PageAndEvent, page: Page, event: Event) = res
            val res_str = pageAndEventParser.combineTuple(user, pageAndEvent, page, event).map(x => x match {
              case y if y == null || y.toString.isEmpty => "\\N"
              case _ => x
            }).mkString("\001")
            val partitionStr = DateUtils.dateGuidPartitions(endTime, gu_id)
            (partitionStr, "PCevent", res_str)
          }
        }
        catch {
          //使用模式匹配来处理异常
          case ex: Exception => {
            println(ex.getStackTraceString)
          }
            println("=======>> PcEvent: getGuid Exception!!" + "======>>异常数据:" + row)
            ("", "", None)
        }
      } else {
        println("=======>> PcEvent: getGuid Exception!!" + "======>>异常数据:" + row)
        ("", "", None)
      }
      ret
    } else {
      println("=======>> PcEvent: getGuid Exception!!" + "======>>异常数据:" + row)
      ("", "", None)
    }
  }

  implicit def javaToScalaInt(d: java.lang.Integer) = d.intValue

  def parse(row: JsValue,
            dimPage: mutable.HashMap[String, (Int, Int, String, Int)],
            dimEvent: mutable.HashMap[String, (Int, Int)]): (User, PageAndEvent, Page, Event) = {

    // ---------------------------------------------------------------- mb_event ----------------------------------------------------------------
    val act_name = (row \ "act_name").asOpt[String].getOrElse("")
    val goodid = (row \ "goodid").asOpt[String].getOrElse("")
    val url = (row \ "url").asOpt[String].getOrElse("")
    val urlref = (row \ "urlref").asOpt[String].getOrElse("")
    val ul_id = (row \ "ul_id").asOpt[String].getOrElse("")
    val ul_idts = (row \ "ul_idts").asOpt[Int].getOrElse(0)
    val ul_ref = (row \ "ul_ref").asOpt[String].getOrElse("")
    val s_uid = (row \ "s_uid").asOpt[String].getOrElse("")
    val utmId = (row \ "utm").asOpt[String].getOrElse("")
    val timeStamp = (row \ "timestamp").asOpt[String].getOrElse("")
    val sessionid = (row \ "sessionid").asOpt[String].getOrElse("")
    val click_action_name = (row \ "click_action_name").asOpt[String].getOrElse("")
    val click_url = (row \ "click_url").asOpt[String].getOrElse("")
    val qm_device_id = (row \ "qm_device_id").asOpt[String].getOrElse("")
    val action_type = (row \ "action_type").asOpt[String].getOrElse("")
    val action_name = (row \ "action_name").asOpt[String].getOrElse("")
    val e_n = (row \ "e_n").asOpt[String].getOrElse("")
    val e_v = (row \ "e_v").asOpt[String].getOrElse("")
    val ip = (row \ "ip").asOpt[String].getOrElse("")
    val qm_session_id = (row \ "qm_session_id").asOpt[String].getOrElse("")
    val qm_jpid = (row \ "qm_jpid").asOpt[String].getOrElse("")

    val baseUrl = if ("".equals(click_url)) {
      url
    } else {
      click_url
    }

    val basebaseUrlRef = if ("".equals(click_url)) {
      urlref
    } else {
      url
    }

    val baseTerminalId = getTerminalIdFromBase(qm_device_id, baseUrl)

    val eventJionKey = action_name + "-dw-" + action_type

    val actName = if ("".equals(click_action_name)) {
      action_name
    } else {
      click_action_name
    }

    val ulQt = ul_idts * 1000

    val eV = if ("goodsid".equals(e_n)) {
      new GetGoodsId().evaluate(e_v)
    } else {
      e_v
    }

    val parsedTuple = (actName, utmId, goodid, baseUrl, basebaseUrlRef, ul_id, ul_idts, ul_ref, s_uid, timeStamp, sessionid, click_action_name, click_url, qm_device_id, action_type, action_name, e_n, eV, ip, qm_session_id, qm_jpid)

    if (qm_device_id.length > 6 && baseTerminalId == 2) {
      // m.域名且带有设备号的为APP H5页面
      parseAppH5(dimEvent, dimPage, parsedTuple, baseUrl, basebaseUrlRef)
    } else {
      // 非M.域名或者设备ID长度小于7的为PC/WAP/WX
      // (length(qm_device_id) <= 6 or terminal_id <> 2)) a
      val sid = (row \ "sid").asOpt[String].getOrElse("")
      parsePcWapWx(dimEvent, dimPage, parsedTuple, baseUrl, basebaseUrlRef， eventJionKey, sid)
    }

    val baseUrlPageId = new GetPageID().evaluate(baseUrl)
    val baseRefPageId = new GetPageID().evaluate(basebaseUrlRef)

    val baseUrlPageValue = getPageValue(javaToScalaInt(baseUrlPageId), baseUrl)
    val baseRefPageValue = getPageValue(javaToScalaInt(baseRefPageId), basebaseUrlRef)

    null
  }

  def getDwSiteId(baseUrl: String): Int = {
    if (!baseUrl.isEmpty) {
      val id: java.lang.Integer = new GetSiteId().evaluate(baseUrl)
      val siId: Int = javaToScalaInt(id)
      if (siId == ScalaConstants.siteJuanpi
        || siId == ScalaConstants.siteJiuKuaiYou
        || siId == ScalaConstants.siteAll
      ) {
        ScalaConstants.siteAll
      } else {
        ScalaConstants.siteUnknow
      }
    } else ScalaConstants.siteUnknow
  }

  def getPageLevel2Value(pageId: String, shopId: String, baseUrl: String): String = {
    val pageLevel2Value = if (pageId == "10104") {
      val skcId = new GetSkcId().evaluate(baseUrl)
      skcId
    } else if (pageId == "10102") {
      shopId
    } else if (baseUrl.contains("singlemessage")) {
      "singlemessage"
    } else if (baseUrl.contains("groupmessage")) {
      "groupmessage"
    } else if (baseUrl.contains("timeline")) {
      "timeline"
    } else {
      ""
    }
    pageLevel2Value
  }

  // 解析app端 h5 端数据
  def parseAppH5(dimEvent: mutable.HashMap[String, (Int, Int)],
                 dimPage: mutable.HashMap[String, (Int, Int, String, Int)],
                 parsedTuple: _, baseUrlRef: String, baseUrl: String): Unit = {

//                 , qm_device_id: String, qm_session_id: String, qm_jpid: String,
//                 ul_id: String, actionName: String, eventJionKey: String, e_v: String): Unit = {
    val (actName, utmId, goodid, url, urlref, ul_id, ul_idts, ul_ref, s_uid, utm, timeStamp, sessionid, click_action_name, click_url, qm_device_id, action_type, action_name, e_n, e_v, ip, qm_session_id, qm_jpid) = parsedTuple

    // H5页面的gu_id通过cookie中捕获APP的gu_id获取
    val guId = if (qm_device_id.isEmpty) {
      ul_id
    } else {
      qm_jpid
    }

    val dwTeminalId = getTerminalIdForH5(qm_device_id)

    val appVersion = ""

    val (d_event_id: Int, event_type_id: Int) = dimEvent.get(eventJionKey).getOrElse(0, 0)
    val eventId = d_event_id match {
      case a if a > 0 => a
      case _ => 0
    }

    val eventValue = if (event_type_id == 10) {
      val ev0 = e_v.split("::")(0)
      val ev1 = e_v.split("::")(1)
      val res = new GetDwPcPageValue().evaluate(ev1)
      actionName + "::" + res + "::" + ev0
    } else {
      e_v
    }

    val refPageId = new GetPageID().evaluate(baseUrlRef)
    val refPageValue = new GetDwPcPageValue().evaluate(baseUrlRef)
    val refSiteId = new GetSiteId().evaluate(baseUrlRef)
    val pageId = new GetPageID().evaluate(baseUrl)
    val pageValue = new GetDwPcPageValue().evaluate(baseUrl)
    val shopId = new GetShopId().evaluate(baseUrl)
    val refShopId = new GetShopId().evaluate(baseUrlRef)
    val (d_page_id: Int, page_type_id: Int, d_page_value: String, d_page_level_id: Int) = dimPage.get(pageId.toString).getOrElse(0, 0, "", 0)
    val pageLevelId = d_page_level_id

    val pageLevel2Value = getPageLevel2Value(pageId.toString, shopId, baseUrl)
    val refPageLevel2Value = getPageLevel2Value(pageId.toString, shopId, baseUrlRef)
    val eventLevel2Vlue = ""

    val location = ""
    val ctag = ""
    val rule_id = ""
    val test_id = ""
    val select_id = ""
    val jpk = 0
    val pit_type = 0
    val sortdate = ""
    val sorthour = "0"
    val lplid = "0"
    val ptplid = "0"
    val gid = ""
    val ugroup = ""
    val loadTime = "0"
    val source = ""
    val ip = ""
    val hotGoodsId = ""
    val deviceId = ""
    val to_switch = ""
    val utm = ""
    val date = ""
    val hour = ""
    val uid = ""

    val dwSiteId = getDwSiteId(baseUrl)
    val dwSessionId = getDwSessionId(qm_session_id, qm_jpid)

    val table_source = "h5_app_event"
    //    (dwTeminalId, appVersion, eventId, dwSiteId, dwSessionId,)
    val user = User.apply(guId, uid, utm, "", dwSessionId, dwTeminalId, appVersion, dwSiteId, javaToScalaInt(refSiteId), ctag, location, jpk, ugroup, date, hour)
    val pe = PageAndEvent.apply(javaToScalaInt(pageId), pageValue, javaToScalaInt(refPageId), refPageValue, shopId, refShopId, pageLevelId, "0", "0", hotGoodsId, pageLevel2Value, refPageLevel2Value, pit_type, sortdate, sorthour, lplid, ptplid, gid, table_source)
    val page = Page.apply(source, ip, "", "", deviceId, to_switch)
    val event = Event.apply(eventId.toString(), eventValue, eventLevel2Vlue, rule_id, test_id, select_id, loadTime)
  }

  def getDwSessionId(sId: String, guId: String): String = {
    //  -- H5页面的session_id通过cookie中捕获APP的session_id获取
    val dwSessionId = if (!sId.isEmpty) {
      val res = sId match {
        case "null" => guId
        case x if x.length > 0 => x
        case _ => guId
      }
      res
    } else {
      guId
    }
    dwSessionId
  }


  def getTerminalIdFromBase(qm_device_id: String, url: String): Int = {
    import scala.util.matching._
    val reg = new Regex("""http(s?)://(tuan|wx).*""")
    val terminalId = if ("MicroMessenger".equals(qm_device_id)) {
      val res = url match {
        case reg(x, y) => 6
        case _ => 1
      }
      res
    } else {
      val reg = new Regex("""http(s?)://(tuan|m).*""")
      val res = url match {
        case reg(x, y) => 2
        case _ => 1
      }
      res
    }
    terminalId
  }

  def getTerminalIdForPC(terminalId: Int): Int = {
    terminalId match {
      case 1 => ScalaConstants.T_PC
      case 2 => ScalaConstants.T_Wap
      case 6 => ScalaConstants.T_WeiXin
      case _ => ScalaConstants.T_Unknow
    }
  }

  def getTerminalIdForH5(qmDeviceId: String): Int = {
    qmDeviceId.length match {
        case 14 | 15 => ScalaConstants.T_Android
        case 36 => ScalaConstants.T_IOS
        case _ => ScalaConstants.T_Unknow
    }
  }

  def getPageValue(urlPageId: Int, url: String): String = {
    val upa = Array(12, 14, 25, 26, 28, 29)
    val pageValue = urlPageId match {
      case 33 => new GetKeyWord().evaluate(url)
      case x if (upa.exists({ x: Int => x == urlPageId })) => new GetKeyWord().evaluate(url)
      case _ => ""
    }
    pageValue
  }

  /**
    * WHEN b.event_type_id = 10 THEN concat(action_name,'::',getdwpcpagevalue(split(a.event_value,'::')[1]),'::',split(a.event_value,'::')[0])
    * @param event_type_id
    * @param actionName
    * @param e_v
    * @return
    */
  def getEventValue(event_type_id: Int, actionName: String, e_v: String): String = {
    val eventValue = if (event_type_id == 10) {
      val ev1 = e_v.split("::")(1)
      val res = new GetDwPcPageValue().evaluate(ev1)
      val ev0 = e_v.split("::")(0)
      actionName + "::" + res + "::" + ev0
    } else {
      e_v
    }
    eventValue
  }

    // 解析pc wap wx 端数据
  def parsePcWapWx (dimEvent: mutable.HashMap[String, (Int, Int)],
                    dimPage: mutable.HashMap[String, (Int, Int, String, Int)],parsedTuple: _,
//                    baseUrlRef: String, baseUrl: String,
                    eventJionKey: String, sid: String): Unit = {

//  (dimEvent: mutable.HashMap[String, (Int, Int)],
//                    dimPage: mutable.HashMap[String, (Int, Int, String, Int)],
//                    baseUrlRef: String, baseUrl: String, qm_device_id: String, qm_session_id: String, qm_jpid: String,
//                    ul_id: String, actionName: String, eventJionKey: String, e_v: String, sid: String): Unit = {

    val (actionName, utmId, goodid, baseUrl, baseUrlRef, ul_id, ul_idts, ul_ref, s_uid, utm, timeStamp, sessionid, click_action_name, click_url, qm_device_id, action_type, action_name, e_n, e_v, ip, qm_session_id, qm_jpid) = parsedTuple

    val baseTerminalId = getTerminalIdFromBase(qm_device_id, baseUrl)
    val dwTeminalId = getTerminalIdForPC(baseTerminalId)
    val app_version = ""
    val (d_event_id: Int, event_type_id: Int) = dimEvent.get(eventJionKey).getOrElse(0, 0)
    val eventId = d_event_id match {
      case a if a > 0 => a
      case _ => 0
    }
    val dwSiteId = getDwSiteId(baseUrl)
    val guId = ul_id
    val dwSessionId = getDwSessionId(sid, ul_id)
    val eventValue = getEventValue(event_type_id, actionName, e_v)
    val refPageId = new GetPageID().evaluate(baseUrlRef)
    val refPageValue = new GetDwPcPageValue().evaluate(baseUrlRef)
    val refSiteId = new GetSiteId().evaluate(baseUrlRef)
    // hive-udf Decoding函数
    val userId = Decoding.evaluate(s_uid)
    val pageId = new GetPageID().evaluate(baseUrl)
    val pageValue = new GetDwPcPageValue().evaluate(baseUrl)
    val shopId = new GetShopId().evaluate(baseUrl)
    val refShopId = new GetShopId().evaluate(baseUrlRef)
    val starttime = timeStamp
    val endtime = timeStamp

    val table_source = "h5_app_event"

    val appVersion = ""


    // TODO 以 page_id 为key

    val (d_page_id: Int, page_type_id: Int, d_page_value: String, d_page_level_id: Int) = dimPage.get(pageId.toString).getOrElse(0, 0, "", 0)
    val pageLevelId = d_page_level_id
    val pageLevel2Value = ""

    val refPageLevel2Value = ""
    val eventLevel2Vlue = ""
    val jpk = 0
    val pit_type = 0
    val sortdate = ""
    val sorthour = "0"
    val lplid = "0"
    val ptplid = "0"
    val gid = ""
    val ugroup = ""
    val loadTime = "0"
    val source = ""
    val ip = ""
    val hotGoodsId = ""

    val ctag = ""
    val location = ""
    val deviceId = ""
    val to_switch = ""
    val utm = ""
    val date = ""
    val hour = ""

    val rule_id = ""
    val test_id = ""
    val select_id = ""

    //    (dwTeminalId, appVersion, eventId, dwSiteId, dwSessionId,)
    val user = User.apply(guId, userId.toString, utmId, "", dwSessionId, dwTeminalId, appVersion, dwSiteId, javaToScalaInt(refSiteId), ctag, location, jpk, ugroup, date, hour)
    val pe = PageAndEvent.apply(javaToScalaInt(pageId), pageValue, javaToScalaInt(refPageId), refPageValue, shopId, refShopId, pageLevelId, "0", "0", hotGoodsId, pageLevel2Value, refPageLevel2Value, pit_type, sortdate, sorthour, lplid, ptplid, gid, table_source)
    val page = Page.apply(source, ip, "", "", deviceId, to_switch)
    val event = Event.apply(eventId.toString(), eventValue, eventLevel2Vlue, rule_id, test_id, select_id, loadTime)

    (user, pe, page, event)
  }
}


object H5EventTransformer {

  def main(args: Array[String]): Unit = {
    val h5 = new H5EventTransformer()

    val urlPageId = 34
    val upa = Array(12, 14, 25, 26, 28, 29)
    val res = urlPageId match {
      case 33 => "aaaa"
      case x if (upa.exists({ x: Int => x == urlPageId })) => "bbbb"
      case _ => ""
    }
    println(res)
  }
}
