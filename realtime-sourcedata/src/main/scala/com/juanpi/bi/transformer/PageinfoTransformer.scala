package com.juanpi.bi.transformer

import com.alibaba.fastjson.JSON
import com.juanpi.bi.init.InitConfig
import com.juanpi.bi.sc_utils.DateUtils
import com.juanpi.bi.streaming.DateHour
import com.juanpi.bi.utils.{GetGoodsId, GetMbPageId}
import org.apache.derby.vti.Restriction.OR
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Get, HTable, Put, Result}
import org.apache.spark.rdd.RDD
import play.api.libs.json.{JsValue, Json}
import org.apache.hadoop.hbase.util.Bytes
/**
  * 解析逻辑的具体实现
  */
class PageinfoTransformer extends ITransformer {

  val hbase_family = "dw"
  val ic = new InitConfig
  val table = TableName.valueOf("ticks_history")

  def parse(row: JsValue): String = {
    // mb_pageinfo
    val ticks = (row \ "ticks").asOpt[String].getOrElse("")
    val session_id = (row \ "session_id").asOpt[String].getOrElse("")
    val pagename = (row \ "pagename").asOpt[String].getOrElse("").toLowerCase()
    val starttime = (row \ "starttime").asOpt[String].getOrElse("0")
    val endtime = (row \ "endtime").asOpt[Long].getOrElse(0)
    val pre_page = (row \ "pre_page").asOpt[String].getOrElse("")
    val uid = (row \ "uid").asOpt[String].getOrElse("0")
    val extend_params = (row \ "extend_params").asOpt[String].getOrElse("")
    val app_name = (row \ "app_name").asOpt[String].getOrElse("")
    val app_version = (row \ "app_version").asOpt[String].getOrElse("")
    val os_version = (row \ "os_version").asOpt[String].getOrElse("")
    val os = (row \ "os").asOpt[String].getOrElse("")
    var utm = (row \ "utm").asOpt[String].getOrElse("0")
    val source = (row \ "source").asOpt[String].getOrElse("")
    val starttime_origin = (row \ "starttime_origin").asOpt[String].getOrElse("")
    val endtime_origin = (row \ "endtime_origin").asOpt[String].getOrElse("")
    val pre_extend_params = (row \ "pre_extend_params").asOpt[String].getOrElse("")
    val wap_url = (row \ "wap_url").asOpt[String].getOrElse("")
    val wap_pre_url = (row \ "wap_pre_url").asOpt[String].getOrElse("")
    val deviceid = (row \ "deviceid").asOpt[String].getOrElse("")
    val jpid = (row \ "jpid").asOpt[String].getOrElse("")
    val ip = (row \ "ip").asOpt[String].getOrElse("")
    val to_switch = (row \ "to_switch").asOpt[Int].getOrElse("0")
    val location = (row \ "location").asOpt[String].getOrElse("")
    val c_label = (row \ "c_label").asOpt[String].getOrElse("")
    val server_jsonstr = (row \ "server_jsonstr").asOpt[String].getOrElse("")
    val c_server = (row \ "c_server").asOpt[String].getOrElse("")

    // mb_pageinfo -> mb_pageinfo_log
    val (extend_params_1, pre_extend_params_1) = pagename.toLowerCase() match {
      case "page_goods" | "page_temai_goods" | "page_temai_imagetxtgoods" | "page_temai_parametergoods" => {
        (GetGoodsId.evaluate(extend_params), GetGoodsId.evaluate(pre_extend_params))
      }
      case _ => {
        (extend_params.toLowerCase(), pre_extend_params.toLowerCase())
      }
    }

    // TODO GetMbPageId 函数需要更新
    val pageId = GetMbPageId.evaluate(pagename.toLowerCase(), extend_params_1)
    val goodsId = if ((pageId == 158) ||
      (pageId == 159 && (app_version == "3.2.3" || app_version == "3.2.4") && (os.toLowerCase() == "ios"))) {
      extend_params_1
    } else {
      "-1"
    }

    var gu_create_time = ""

    val gu_id = os.toLowerCase match {
      case "android" => jpid
      case "ios" => deviceid
      case _ => "0"
    }

    val terminal_id = os.toLowerCase match {
      case "android" => 2
      case "ios" => 3
      case _ => -999
    }

    // todo 查hbase 从 ticks_history 中查找 ticks 存在的记录

    val ticks_history = ic.getHbaseConf().getTable(table)

    //查询某条数据
    val key = new Get(Bytes.toBytes(ticks + app_name))
    val ticks_res = ticks_history.get(key)

    if (!ticks_res.isEmpty) {
      utm = Bytes.toString(ticks_res.getValue(hbase_family.getBytes, "utm".getBytes))
      gu_create_time = Bytes.toString(ticks_res.getValue(hbase_family.getBytes, "gu_create_time".getBytes))
    }
    else {
      // 如果不存在就写入 hbase
      // 准备插入一条 key 为 id001 的数据
      val p = new Put((ticks + app_name).getBytes)
      // 为put操作指定 column 和 value （以前的 put.add 方法被弃用了）
      p.addColumn(hbase_family.getBytes, "utm".getBytes, utm.getBytes)
      p.addColumn(hbase_family.getBytes, "gu_create_time".getBytes, gu_create_time.getBytes)
      //提交
      ticks_history.put(p)
    }

    val logTime = if (starttime.size == 0) {
      0L
    } else {
      starttime.toLong
    }

    val validGoodsId = try {
      if (goodsId.size == 0) {
        "-1"
      } else {
        val goods = goodsId.toInt
        goods.toString
      }
    } catch {
      case ex: NumberFormatException => {
        println("======>> pageinfo解析异常" + ":" + goodsId + ":" + row)
        "-1"
      }
    }

    // gu_id
    val id = if ((uid.length == 0) || uid.equals("0")) {
      gu_id
    } else {
      uid
    }

    val site_id = app_name.toLowerCase match {
      case "jiu" => 2
      case "zhe" => 1
      case _ => -999
    }

    val ref_site_id = site_id

    // session_id 判断
    val sess_id = session_id match
    {
      case ("" | "null" ) => ticks
      case session_id if session_id.length() ==0 => ticks
      case _ => ticks
    }

    // 最终返回值
    return ""
  }

  // 返回解析的结果
  def transform(line: String, page: RDD[(Int, (String, String))], event: RDD[(Int, (String, String))]): (String, String) = {

    // fastjson 也可以用。
    // val row = JSON.parseObject(line)

    //play
    val row = Json.parse(line)
    // 解析逻辑
    val res = parse(row)

    if (row != null) {
      (DateUtils.dateHour((row \ "endtime").as[Long]).toString, res.toString())
    } else {
      (DateHour("1970-01-01", "1").toString, line)
    }
  }
}

// for test
object PageinfoTransformer{
  def main(args: Array[String]) {
    val liuliang = """{"session_id":"1448943287122_jiu_1457400715757","ticks":"1448943287122","uid":"29617028","utm":"102524","app_name":"jiu","app_version":"3.4.0","os":"android","os_version":"5.1","deviceid":"867905020977700","jpid":"ffffffff-e5c4-5627-25f1-ba271bc27f63","to_switch":"1","location":"湖南省","c_label":"C2","source":"","starttime":"1457400759215","endtime":"1457400761605","pagename":"page_h5","extend_params":"http://m.juanpi.com/help/customer","pre_page":"page_center","pre_extend_params":"","wap_url":"http://m.juanpi.com/help/customer","wap_pre_url":"about:blank","gj_page_names":"page_tab,page_tab","gj_ext_params":"all,all","starttime_origin":"1457400758719","endtime_origin":"1457400761109","ip":"111.8.228.232"}"""
    val b = JSON.parseObject(liuliang)
    println(b.get("session_id"))

    val row = Json.parse(liuliang)
    println(row)
  }
}