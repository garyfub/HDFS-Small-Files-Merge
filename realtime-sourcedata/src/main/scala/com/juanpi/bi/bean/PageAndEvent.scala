package com.juanpi.bi.bean

/**
  * Created by gongzi on 2016/7/7.
  */

case class User (
                  gu_id: String,
                  utm_id: String,
                  gu_create_time: String
                )

case class PageAndEvent (
  // page and event 公共属性
  session_id: String,
  terminal_id: Int,
  app_version: String,
  page_id: Int,
  page_value: String,
  site_id: Int,
  ref_site_id: Int,
  ref_page_id: Int,
  ref_page_value: String,
  page_level_id: Int,
  starttime: String,
  endtime: String,
  ctag: String,
  hot_goods_id: String,
  page_lvl2_value: String,
  ref_page_lvl2_value: String,
  jpk: Int,
  pit_type: Int,
  sortdate: String,
  sorthour: String,
  lplid: String,
  ptplid: String,
  gid: String,
  ugroup: String)

case class Page(

  // page 独有属性
  source: String,
  ip: String,
  url: String,
  urlref: String,
  deviceid: String,
  to_switch: String)

case class Event(
  // event 独有属性
  event_id: String,
  event_value: String,
  event_lvl2_value: String,
  rule_id: String,
  test_id: String,
  select_id: String
)
