package com.juanpi.bi.transformer

import com.juanpi.bi.bean.{Event, Page, PageAndEvent, User}
import com.juanpi.bi.init.ScalaConstants
import com.juanpi.bi.sc_utils.{DateUtils, TimeUtils}
import com.juanpi.hive.udf._
import play.api.libs.json.{JsValue, Json}

import scala.collection.mutable

/**
  * Created by gongzi on 2017/2/4.
  */
class H5PageTransformer {

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
                     ip: String,
                     qm_session_id: String,
                     qm_jpid: String)

  def logParser(line: String,
                dimPage: mutable.HashMap[String, (Int, Int, String, Int)]
               ): (String, String, Any) = {

    val row = Json.parse(line)

    // web 端 gu_id 从ul_id来，H5页面的gu_id通过cookie中捕获APP的gu_id获取
    val qm_jpid = (row \ "qm_jpid").asOpt[String].getOrElse("")
    val ul_id = (row \ "ul_id").asOpt[String].getOrElse("")
    val timeStamp = (row \ "timestamp").as[String].toLong

    val gu_id = if(ul_id.isEmpty() && qm_jpid.isEmpty) {
      ""
    } else if(ul_id.isEmpty) {
      qm_jpid
    } else {
      ul_id
    }

    val ret = if (gu_id.nonEmpty && !gu_id.equalsIgnoreCase("null")) {
      val endtime = (row \ "endtime").asOpt[String].getOrElse("")
      val server_jsonstr = (row \ "server_jsonstr").asOpt[String].getOrElse("")

      try {
        val res = parse(row, dimPage)
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
          val partitionStr = DateUtils.dateGuidPartitions(timeStamp, gu_id)
          (partitionStr, "h5_page", res_str)
        }
      }
      catch {
        //使用模式匹配来处理异常
        case ex: Exception => {
          println(ex.getStackTraceString)
        }
          println("=======>> h5_page: getGuid Exception!!" + "======>>异常数据:" + row)
          ("", "", None)
      }
    } else {
      println("=======>> PcPage: getGuid Exception!!" + "======>>异常数据:" + row)
      ("", "", None)
    }
    ret
  }

  /**
    *
    * @param qm_device_id
    * @param url
    * @return 6:微信端；2: m站 ; 1: pc
    *
    **/
  def getTerminalIdFromBase(qm_device_id: String, url: String): Int = {
    import scala.util.matching._
    val reg = new Regex("""http(s?)://(tuan|kan).*""")
    val terminalId = if ("MicroMessenger".equals(qm_device_id)) {
      val res = url match {
        case reg(x, y) => 6
        case _ => 1
      }
      res
    } else if(url matches("http(s)?://wx.juanpi.com.*")) {
      6
    } else if(url matches("http(s)?://(mact|tuan|m|mapi|kan).juanpi.com.*")) {
      2
    } else 1
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

  def getPageLevel2Value(baseUrl: String): String = {
    val pageLevel2Value = if (baseUrl.contains("singlemessage")) {
      "singlemessage"
    } else if (baseUrl.contains("groupmessage")) {
      "groupmessage"
    } else if (baseUrl.contains("timeline")) {
      "timeline"
    } else if (baseUrl.contains("pt_src")) {
      val regex = """pt_src=(.*?_?(\w+|\d+))""".r
      val p1 = for (m <- regex.findFirstMatchIn(baseUrl)) yield m.group(1)
      p1.get
    } else {
      ""
    }
    pageLevel2Value
  }

  def parse(row: JsValue,
            dimPage: mutable.HashMap[String, (Int, Int, String, Int)]
           ): (User, PageAndEvent, Page, Event) = {

    // ---------------------------------------------------------------- mb_page ----------------------------------------------------------------
    val action_name = (row \ "action_name").asOpt[String].getOrElse("")
    val url = (row \ "url").asOpt[String].getOrElse("")
    val utmid = (row \ "utmid").asOpt[String].getOrElse("")
    val dutmid = (row \ "dutmid").asOpt[String].getOrElse("")
    val goodid = (row \ "goodid").asOpt[String].getOrElse("")
    val dgoodid = (row \ "dgoodid").asOpt[String].getOrElse("")
    val urlref = (row \ "urlref").asOpt[String].getOrElse("")
    val keyword = (row \ "keyword").asOpt[String].getOrElse("")
    val ul_id = (row \ "ul_id").asOpt[String].getOrElse("")
    val ul_idts = (row \ "ul_idts").asOpt[Int].getOrElse(0)
    val jpk = (row \ "jpk").asOpt[Int].getOrElse(0)
    val ul_ref = (row \ "ul_ref").asOpt[String].getOrElse("")
    val ul_qt = (row \ "ul_qt").asOpt[String].getOrElse("")
    val s_uid = (row \ "s_uid").asOpt[String].getOrElse("")
    val sid = (row \ "sid").asOpt[String].getOrElse("")
    val utm = (row \ "utm").asOpt[String].getOrElse("")
    val timeStamp = (row \ "timestamp").asOpt[String].getOrElse("0")
    val sessionid = (row \ "sessionid").asOpt[String].getOrElse("")
    val click_action_name = (row \ "click_action_name").asOpt[String].getOrElse("")
    val click_url = (row \ "click_url").asOpt[String].getOrElse("")
    val qm_device_id = (row \ "qm_device_id").asOpt[String].getOrElse("")
    val ip = (row \ "ip").asOpt[String].getOrElse("")
    val qm_session_id = (row \ "qm_session_id").asOpt[String].getOrElse("")
    val qm_jpid = (row \ "qm_jpid").asOpt[String].getOrElse("")

    val baseUrl = if ("".equals(click_url)) {
      url
    } else {
      click_url
    }

    val pid = new GetPageID().evaluate(baseUrl)
    val pageId = if(pid == null) {0} else javaToScalaInt(pid)

    val (d_page_id: Int, page_type_id: Int, d_page_value: String, d_page_level_id: Int) = dimPage.get(pageId.toString).getOrElse(0, 0, "", 0)

    // todo WATCHOUT! 数据与dim_page是通过join关联的，如果没有匹配上，返回null
    if(d_page_id == 0) {
      return null
    }

    val baseUrlRef = if ("".equals(click_url)) {
      urlref
    } else {
      url
    }

    val actName = if ("".equals(click_action_name)) {
      action_name
    } else {
      click_action_name
    }

    val utmId = if(jpk > 0) { jpk+"" } else utmid

    val ulQt = ul_idts * 1000

    // hive-udf Decoding函数 Decoding.(baseLog.s_uid)
    val userId = Decoding.evaluate(s_uid)

    val baseSiteId = new GetSiteId().evaluate(baseUrl)

    val refPid = new GetPageID().evaluate(baseUrlRef)
    val refUrlPageId = if(refPid == null) {0} else javaToScalaInt(refPid)

    val pageValue = new GetDwPcPageValue().evaluate(baseUrl)
    val refPageId = new GetPageID().evaluate(baseUrlRef)
    val refPageValue = new GetDwPcPageValue().evaluate(baseUrlRef)

    val dwSiteId = if(baseSiteId == null || baseSiteId == 0 || baseSiteId == "null") -999 else javaToScalaInt(baseSiteId)

    val refSiteId = new GetSiteId().evaluate(baseUrlRef)

    val baseTerminalId = getTerminalIdFromBase(qm_device_id, baseUrl)
    val dwTerminalId = getTerminalIdForPC(baseTerminalId)
    val appVersion = ""
    val guId = ul_id

    val dwSessionId = if(sid == null || sid.length == 0 || sid == "null") ul_id else sid

    val shopId = new GetShopId().evaluate(baseUrl)
    val refShopId = new GetShopId().evaluate(baseUrlRef)

    val location = ""
    val ctag = ""

    val pageLevel2Value = getPageLevel2Value(baseUrl)
    val refPageLevel2Value = getPageLevel2Value(baseUrlRef)

    val gu_create_time = DateUtils.dateStr(timeStamp.toLong)

    val pit_type = 0
    val sortdate = ""
    val sorthour = "0"
    val lplid = "0"
    val ptplid = "0"
    val gid = ""
    val ugroup = ""
    val source = ""
    val hotGoodsId = ""

    val deviceId = ""
    val to_switch = ""
    val (date, hour) = TimeUtils.dateHourFormat(timeStamp)

    val pageLevelId = d_page_level_id

    val startTime, endTime = timeStamp

    val eventId, eventValue, eventLevel2Vlue, rule_id, test_id, select_id = ""
    val loadTime = "0"
    val ug_id = ""

    val table_source = "h5_page"

    //    val log = BaseLog(actName, utmId, goodid, baseUrl, baseUrlRef, ul_id, ul_idts,
//      ul_ref, s_uid, timeStamp, sessionid, click_action_name, click_url,
//      qm_device_id, actionType, action_name, ip, qm_session_id, qm_jpid)

//    val res = if (qm_device_id.length <= 6 || baseTerminalId != 2) {

    val user = User.apply(guId, userId.toString, utmId, "", dwSessionId, dwTerminalId, appVersion, dwSiteId, javaToScalaInt(refSiteId), ctag, location, jpk, ugroup, date, hour)
    val pe = PageAndEvent.apply(javaToScalaInt(pageId), pageValue, javaToScalaInt(refPageId), refPageValue, shopId, refShopId, pageLevelId, startTime, endTime, hotGoodsId, pageLevel2Value, refPageLevel2Value, pit_type, sortdate, sorthour, lplid, ptplid, gid, table_source)
    val page = Page.apply(source, ip, "", "", deviceId, to_switch)
    val event = Event.apply(eventId, eventValue, eventLevel2Vlue, rule_id, test_id, select_id, loadTime, ug_id)
    (user, pe, page, event)
  }
}

object H5PageTransformer {
  def main(args: Array[String]): Unit = {
    val url = "http://tuan.juanpi.com/pintuan?id=367&pt_src=ggmk_1"
    val url2 = "https://mapi.juanpi.com/h5/attr?id=42928995&goods_id=35774050 "
    val h5 = new H5PageTransformer()
    val res = h5.getPageLevel2Value(url)
    val res1= h5.getTerminalIdFromBase("M",url2)
    println(res)
    println(res1)
  }
}
