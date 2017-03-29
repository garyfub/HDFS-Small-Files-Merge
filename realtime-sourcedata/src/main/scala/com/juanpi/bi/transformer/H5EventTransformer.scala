package com.juanpi.bi.transformer
import com.juanpi.bi.bean.{Event, Page, PageAndEvent, User}
import com.juanpi.bi.init.ScalaConstants
import com.juanpi.bi.sc_utils.{DateUtils, TimeUtils}
import com.juanpi.hive.udf._
import play.api.libs.json.{JsValue, Json}

import scala.collection.mutable

/**
  * Created by gongzi on 2016/11/28.
  */
class H5EventTransformer {

  implicit def javaToScalaInt(d: java.lang.Integer) = d.intValue

  case class BaseLog(actName: String,
                     utmId: String,
                     goodid: String,
                     baseUrl: String,
                     baseUrlRef: String,
                     ul_id: String,
                     ul_idts: Int,
                     ul_ref: String,
                     s_uid: String,
                     timeStamp: String,
                     sessionid: String,
                     click_action_name: String,
                     click_url: String,
                     qm_device_id: String,
                     actionType: String,
                     actionName: String,
                     e_n: String,
                     eV: String,
                     ip: String,
                     qm_session_id: String,
                     qm_jpid: String)


  def logParser(line: String,
                dimPage: mutable.HashMap[String, (Int, Int, String, Int)],
                dimEvent: mutable.HashMap[String, (Int, Int, Int)],
                startDateStr: String, endDateStr: String): (String, String, Any) = {

    val row = Json.parse(line)

    // web 端 gu_id 从ul_id来，H5页面的gu_id通过cookie中捕获APP的gu_id获取
    val qm_jpid = (row \ "qm_jpid").asOpt[String].getOrElse("")
    val ul_id = (row \ "_id").asOpt[String].getOrElse("")
    val timeStamp = (row \ "timestamp").as[String]

    if(timeStamp.isEmpty) {
      return ("", "", null)
    }

    // pc端wap数据 APP端H5点击
    val qm_device_id=(row \ "qm_device_id").asOpt[String].getOrElse("")
    val url = (row \ "url").asOpt[String].getOrElse("")
    val baseTerminal = pageAndEventParser.getTerminalIdFromBase(qm_device_id, url)

    // 如果从日志解析得到的时间不是当前消费的日期，就将该数据过滤掉
    val dateStr = DateUtils.dateStr(timeStamp.toLong)
    // 如果日志时间超出了范围，就过滤掉
    if(dateStr < startDateStr || dateStr > endDateStr){
      return ("", "", null)
    }

    // qm_device_id
    val gu_id = if(qm_device_id.length<=6 || baseTerminal != 2) {
        ul_id
      } else if(qm_device_id.length>6 && baseTerminal == 2 && qm_jpid.isEmpty) {
        ul_id
      } else {
        qm_jpid
    }

    val ret = if (gu_id.nonEmpty && !gu_id.equalsIgnoreCase("null")) {
      val partitionStr = DateUtils.dateGuidPartitions(timeStamp.toLong, gu_id)
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

          // 创建分区，格式：date=2016-12-27/gu_hash=a
//          val partitionStr = DateUtils.dateGuidPartitions(timeStamp, gu_id)
          (partitionStr, "h5_event", res_str)
        }
      }
      catch {
        //使用模式匹配来处理异常
        case ex: Exception => {
          println(ex.getStackTraceString)
        }
          println("=======>> h5_event: getGuid Exception!!" + "======>>异常数据:" + row)
          ("", "", None)
      }
    } else {
      println("=======>> PcEvent: getGuid Exception!!" + "======>>异常数据:" + row)
      ("", "", None)
    }
    ret
  }

  def parse(row: JsValue,
            dimPage: mutable.HashMap[String, (Int, Int, String, Int)],
            dimEvent: mutable.HashMap[String, (Int, Int, Int)]): (User, PageAndEvent, Page, Event) = {

    // ---------------------------------------------------------------- mb_event ----------------------------------------------------------------
    val act_name = (row \ "act_name").asOpt[String].getOrElse("")
    val goodid = (row \ "goodid").asOpt[String].getOrElse("")
    val url = (row \ "url").asOpt[String].getOrElse("")
    val urlref = (row \ "urlref").asOpt[String].getOrElse("")
    // 源数据中是 _id
    val ul_id = (row \ "_id").asOpt[String].getOrElse("")
    // 源数据中是 _idts
    val ul_idts = (row \ "_idts").asOpt[Int].getOrElse(0)
    val ul_ref = (row \ "ul_ref").asOpt[String].getOrElse("")
    val s_uid = (row \ "s_uid").asOpt[String].getOrElse("")
    val utmId = (row \ "utm").asOpt[String].getOrElse("")
    val timeStamp = (row \ "timestamp").asOpt[String].getOrElse("")
    val sessionid = (row \ "sessionid").asOpt[String].getOrElse("")
    val click_action_name = (row \ "click_action_name").asOpt[String].getOrElse("")
    val click_url = (row \ "click_url").asOpt[String].getOrElse("")
    val qm_device_id = (row \ "qm_device_id").asOpt[String].getOrElse("")
    val actionType = (row \ "action_type").asOpt[String].getOrElse("")
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

    val baseUrlRef = if ("".equals(click_url)) {
      urlref
    } else {
      url
    }

    val baseTerminalId = pageAndEventParser.getTerminalIdFromBase(qm_device_id, baseUrl)

    val eventJoinKey = action_name + ScalaConstants.JoinDelimiter + actionType

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

    val log = BaseLog(actName, utmId, goodid, baseUrl, baseUrlRef, ul_id, ul_idts,
      ul_ref, s_uid, timeStamp, sessionid, click_action_name, click_url,
      qm_device_id, actionType, action_name, e_n, eV, ip, qm_session_id, qm_jpid)

    val res = if (qm_device_id.length > 6 && baseTerminalId == 2) {
      // m.域名且带有设备号的为APP H5页面
      parseAppH5(dimEvent, dimPage, log, eventJoinKey)
    } else {
      // 非M.域名或者设备ID长度小于7的为PC/WAP/WX
      // (length(qm_device_id) <= 6 or terminal_id <> 2)) a
      val sid = (row \ "sid").asOpt[String].getOrElse("")
      parsePcWapWx(dimEvent, dimPage, log, eventJoinKey, sid)
    }
    res
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

  def getPageLevelId(eventValue: String, event_level_id: Int, d_page_level_id: Int): Int = {
    val pageLevelId = if (List("app_index_pintuan_75.0", "m_index_pintuan_75.0", "wx_index_pintuan_75.0",
      "app_index_pintuan_55.0", "m_index_pintuan_55.0", "wx_index_pintuan_55.0",
      "app_index_pintuan_2107.0", "m_index_pintuan_2107.0", "wx_index_pintuan_2107.0").contains(eventValue)) {
      5
    } else if (event_level_id > 0) {
      event_level_id
    } else {
      d_page_level_id
    }

    pageLevelId
  }

  /**
    * 解析app端 h5 端数据
    *
    * @param dimEvent
    * @param dimPage
    * @param baseLog
    * @param eventJoinKey
    * @return
    */
  def parseAppH5(dimEvent: mutable.HashMap[String, (Int, Int, Int)],
                 dimPage: mutable.HashMap[String, (Int, Int, String, Int)],
                 baseLog: BaseLog,
                 eventJoinKey: String): (User, PageAndEvent, Page, Event) = {

    val baseUrlRef = baseLog.baseUrlRef
    val baseUrl = baseLog.baseUrl
    // H5页面的gu_id通过cookie中捕获APP的gu_id获取
    val qm_device_id = baseLog.qm_device_id
    val guId = if (qm_device_id.isEmpty) {
      baseLog.ul_id
    } else {
      baseLog.qm_jpid
    }

    val dwTeminalId = getTerminalIdForH5(qm_device_id)

    val appVersion = ""

    val (d_event_id: Int, event_type_id: Int, event_level_id: Int) = dimEvent.get(eventJoinKey).getOrElse(0, 0, 0)
    val eventId = d_event_id match {
      case a if a > 0 => a
      case _ => 0
    }

    val eV = baseLog.eV
    val actionName = baseLog.actionName
    val eventValue = if (event_type_id == 10) {
      val ev0 = eV.split("::")(0)
      val ev1 = eV.split("::")(1)
      val res = new GetDwPcPageValue().evaluate(ev1)
      actionName + "::" + res + "::" + ev0
    } else {
      eV
    }

    val refPageId = getPageIdFromUDF(baseUrlRef)
    val refPageValue = new GetDwPcPageValue().evaluate(baseUrlRef)
    val refSiteId = new GetSiteId().evaluate(baseUrlRef)
    val pageId = getPageIdFromUDF(baseUrl)
    val pageValue = new GetDwPcPageValue().evaluate(baseUrl)
    // 新增xpagevalue和pagevalue
    val xpageValue=baseUrl
    val refxpagevalue=baseUrlRef
    val shopId = new GetShopId().evaluate(baseUrl)
    val refShopId = new GetShopId().evaluate(baseUrlRef)
    val (d_page_id: Int, page_type_id: Int, d_page_value: String, d_page_level_id: Int) = dimPage.get(pageId.toString).getOrElse(0, 0, "", 0)

    val pageLevelId = getPageLevelId(eventValue, event_level_id, d_page_level_id)

    val pageLevel2Value = getPageLevel2Value(pageId.toString, shopId, baseUrl)
    val refPageLevel2Value = getPageLevel2Value(pageId.toString, shopId, baseUrlRef)

    val eventLevel2Value = ""
    val location = ""
    val ctag = ""
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
    val ug_id = ""
    val utm = baseLog.utmId
    val startTime = baseLog.timeStamp
    val endTime = baseLog.timeStamp

    val (date, hour) = TimeUtils.dateHourFormat(startTime)

    val userId = Decoding.evaluate(baseLog.s_uid)
    val dwSiteId = getDwSiteId(baseUrl)
    val dwSessionId = getDwSessionId(baseLog.qm_session_id, baseLog.qm_jpid)

    val (rule_id: String, test_id: String, select_id: String) = if(eventValue.contains("_")){
      val v1 = eventValue.split("_")
      (v1(3), v1(2), v1(1))
    }

    val table_source = "h5_app_event"
    val user = User.apply(guId, userId.toString, utm, "", dwSessionId, dwTeminalId, appVersion, dwSiteId, javaToScalaInt(refSiteId), ctag, location, jpk, ugroup, date, hour)
    val pe = PageAndEvent.apply(pageId, pageValue, refPageId, refPageValue, shopId, refShopId, pageLevelId, startTime, endTime, hotGoodsId, pageLevel2Value, refPageLevel2Value, pit_type, sortdate, sorthour, lplid, ptplid, gid, table_source)
    val page = Page.apply(source, ip, "", "", deviceId, to_switch)
    val event = Event.apply(eventId.toString(), eventValue, eventLevel2Value, rule_id, test_id, select_id, loadTime, ug_id, xpageValue, refxpagevalue)

    (user, pe, page, event)
  }

  def getPageIdFromUDF(param: String): Int = {
    val pid = new GetPageID().evaluate(param)

    val pageId = if(pid == null) {0} else javaToScalaInt(pid)

    pageId
  }


  /**
    *  解析pc wap wx 端数据
    *
    * @param dimEvent
    * @param dimPage
    * @param baseLog
    * @param eventJoinKey
    * @param sid
    * @return
    */
  def parsePcWapWx (dimEvent: mutable.HashMap[String, (Int, Int, Int)],
                    dimPage: mutable.HashMap[String, (Int, Int, String, Int)],
                    baseLog: BaseLog,
                    eventJoinKey: String,
                    sid: String): (User, PageAndEvent, Page, Event) = {

    val baseUrlRef = baseLog.baseUrlRef
    val baseUrl = baseLog.baseUrl
    val qm_device_id = baseLog.qm_device_id

    val baseTerminalId = pageAndEventParser.getTerminalIdFromBase(qm_device_id, baseUrl)
    val dwTerminalId = getTerminalIdForPC(baseTerminalId)
    val (d_event_id: Int, event_type_id: Int, event_level_id) = dimEvent.get(eventJoinKey).getOrElse(0, 0, 0)
    val eventId = d_event_id match {
      case a if a > 0 => a
      case _ => 0
    }

    val dwSiteId = getDwSiteId(baseUrl)
    val ul_id = baseLog.ul_id
    val guId = ul_id
    val dwSessionId = getDwSessionId(sid, ul_id)
    val eventValue = getEventValue(event_type_id, baseLog.actionName, baseLog.eV)
    val refPageId = getPageIdFromUDF(baseUrlRef)
    val refPageValue = new GetDwPcPageValue().evaluate(baseUrlRef)
    val refSiteId = new GetSiteId().evaluate(baseUrlRef)
    // hive-udf Decoding函数 Decoding.(baseLog.s_uid)
    val userId = Decoding.evaluate(baseLog.s_uid)
    val pageId = getPageIdFromUDF(baseUrl)
    val pageValue = new GetDwPcPageValue().evaluate(baseUrl)
    val shopId = new GetShopId().evaluate(baseUrl)
    // 新增xpagevalue和pagevalue
    val xpageValue=baseUrl
    val refxpagevalue=baseUrlRef
    val refShopId = new GetShopId().evaluate(baseUrlRef)
    val startTime = baseLog.timeStamp
    val endTime = baseLog.timeStamp

    val table_source = "pc_wx_event"

    val appVersion = ""

    val (d_page_id: Int, page_type_id: Int, d_page_value: String, d_page_level_id: Int) = dimPage.get(pageId.toString).getOrElse(0, 0, "", 0)

    val pageLevelId = getPageLevelId(eventValue, event_level_id, d_page_level_id)

    val pageLevel2Value = getPageLevel2Value(pageId.toString, shopId, baseUrl)
    val refPageLevel2Value = getPageLevel2Value(pageId.toString, shopId, baseUrlRef)

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
    val ug_id = ""
    val hotGoodsId = ""

    val ctag = ""
    val location = ""
    val deviceId = ""
    val to_switch = ""

    val (date, hour) = TimeUtils.dateHourFormat(startTime)

    val (rule_id: String, test_id: String, select_id: String) = if(eventValue.contains("_")){
      val v1 = eventValue.split("_")
      (v1(3), v1(2), v1(1))
    }

    //    (dwTeminalId, appVersion, eventId, dwSiteId, dwSessionId,)
    val user = User.apply(guId, userId.toString, baseLog.utmId, "", dwSessionId, dwTerminalId, appVersion, dwSiteId, javaToScalaInt(refSiteId), ctag, location, jpk, ugroup, date, hour)
    val pe = PageAndEvent.apply(javaToScalaInt(pageId), pageValue, javaToScalaInt(refPageId), refPageValue, shopId, refShopId, pageLevelId, startTime, endTime, hotGoodsId, pageLevel2Value, refPageLevel2Value, pit_type, sortdate, sorthour, lplid, ptplid, gid, table_source)
    val page = Page.apply(source, ip, "", "", deviceId, to_switch)
    val event = Event.apply(eventId.toString(), eventValue, eventLevel2Vlue, rule_id, test_id, select_id, loadTime, ug_id, xpageValue,refxpagevalue)

    (user, pe, page, event)
  }

  /**
    * -- H5页面的session_id通过cookie中捕获APP的session_id获取
    *
    * @param sId
    * @param guId
    * @return
    */
  def getDwSessionId(sId: String, guId: String): String = {
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

  /**
    * WHEN b.event_type_id = 10 THEN concat(action_name,'::',getdwpcpagevalue(split(a.event_value,'::')[1]),'::',split(a.event_value,'::')[0])
    *
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
}
