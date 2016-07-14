package com.juanpi.bi.bean

/**
  * Created by gongzi on 2016/7/7.
  */
class PageAndEvent {

  // page and event 公共属性
  var gu_id: String = ""
  var session_id: String = ""
  var terminal_id: Int = 0
  var utm_id: Int = 0
  var app_version: String = ""
  var page_id: Int = 0
  var page_value: String = ""
  var site_id: Int = 0
  var ref_site_id: Int = 0
  var ref_page_id: String = ""
  var ref_page_value: String = ""
  var page_level_id: Int = 0
  var starttime: BigInt = 0
  var endtime: BigInt = 0
  var ctag: String = ""
  var hot_goods_id: Int = 0
  var page_lvl2_value: String = ""
  var ref_page_lvl2_value: String = ""
  var jpk: Int = 0
  var pit_type: Int = 0
  var sortdate: String = ""
  var sorthour: Int = 0
  var lplid: Int = 0
  var ptplid: Int = 0
  var gid: String = ""
  var ugroup: String = ""

  // page 独有属性
  var source: String = ""
  var ip: String = ""
  var gu_create_time: String = ""
  var url: String = ""
  var urlref: String = ""
  var deviceid: String = ""
  var to_switch: Int = 0

  // event 独有属性
  var event_id: Int = 0
  var event_value: String = ""
  var event_lvl2_value: String = ""
  var rule_id: String = ""
  var test_id: String = ""
  var select_id: String = ""
}
