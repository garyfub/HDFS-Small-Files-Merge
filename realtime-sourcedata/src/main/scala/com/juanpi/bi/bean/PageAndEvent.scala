package com.juanpi.bi.bean

/**
  * Created by gongzi on 2016/7/7.
  */

/**
  * 用户每次访问的基本信息
  * @param gu_id
  * @param utm_id
  * @param gu_create_time
  * @param session_id
  * @param terminal_id
  * @param app_version
  * @param site_id
  * @param ref_site_id
  * @param ctag
  */
case class User (
                  gu_id: String,
                  var utm_id: String,
                  var gu_create_time: String,
                  session_id: String,
                  terminal_id: Int,
                  app_version: String,
                  site_id: Int,
                  ref_site_id: Int,
                  ctag: String
                )

/**
  * 用户访问的页面信息，包括页面和点击
  * @param page_id
  * @param page_value
  * @param ref_page_id
  * @param ref_page_value
  * @param page_level_id
  * @param starttime
  * @param endtime
  * @param hot_goods_id
  * @param page_lvl2_value
  * @param ref_page_lvl2_value
  * @param jpk
  * @param pit_type
  * @param sortdate
  * @param sorthour
  * @param lplid
  * @param ptplid
  * @param gid
  * @param ugroup
  */
case class PageAndEvent (
  // page and event 公共属性
  page_id: Int,
  page_value: String,
  ref_page_id: Int,
  ref_page_value: String,
  page_level_id: Int,
  starttime: String,
  endtime: String,
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
  ugroup: String
                        )

/**
  * 用户访问页面，页面的特殊信息
  * @param source
  * @param ip
  * @param url
  * @param urlref
  * @param deviceid
  * @param to_switch
  */
case class Page(
  // page 独有属性
  source: String,
  ip: String,
  url: String,
  urlref: String,
  deviceid: String,
  to_switch: String
               )

/**
  * 用户访问产生的点击
  * @param event_id
  * @param event_value
  * @param event_lvl2_value
  * @param rule_id
  * @param test_id
  * @param select_id
  */
case class Event(
  // event 独有属性
  event_id: String,
  event_value: String,
  event_lvl2_value: String,
  rule_id: String,
  test_id: String,
  select_id: String
                )
