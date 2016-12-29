package com.juanpi.bi.bean

/**
  * Created by gongzi on 2016/7/7.
  */

/**
  * 用户每次访问的基本信息
  *
  * @param gu_id
  * @param user_id
  * @param utm
  * @param gu_create_time
  * @param session_id
  * @param terminal_id
  * @param app_version
  * @param site_id
  * @param ref_site_id
  * @param ctag
  * @param location
  * @param jpk
  * @param ugroup
  * @param date
  * @param hour
  */
case class User(
                 gu_id: String,
                 user_id: String,
                 var utm: String,
                 var gu_create_time: String,
                 session_id: String,
                 terminal_id: Int,
                 app_version: String,
                 site_id: Int,
                 ref_site_id: Int,
                 ctag: String,
                 location: String,
                 jpk: Int,
                 ugroup: String,
                 date: String,
                 hour: String
               )

/**
  * 用户访问的页面信息，包括页面和点击
  *
  * @param page_id
  * @param page_value
  * @param ref_page_id
  * @param ref_page_value
  * @param shop_id
  * @param ref_shop_id
  * @param page_level_id
  * @param starttime
  * @param endtime
  * @param hot_goods_id
  * @param page_lvl2_value
  * @param ref_page_lvl2_value
  * @param pit_type
  * @param sortdate
  * @param sorthour
  * @param lplid
  * @param ptplid
  * @param gid
  * @param table_source
  */
case class PageAndEvent(
                         // page and event 公共属性
                         page_id: Int,
                         page_value: String,
                         ref_page_id: Int,
                         ref_page_value: String,
                         shop_id: String,
                         ref_shop_id: String,
                         page_level_id: Int,
                         starttime: String,
                         endtime: String,
                         hot_goods_id: String,
                         page_lvl2_value: String,
                         ref_page_lvl2_value: String,
                         pit_type: Int,
                         sortdate: String,
                         sorthour: String,
                         lplid: String,
                         ptplid: String,
                         gid: String,
                         table_source: String
                       )

/**
  * 用户访问页面，页面的特殊信息
  *
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
  *
  */
case class Event(
                  // event 独有属性
                  event_id: String,
                  event_value: String,
                  event_lvl2_value: String,
                  rule_id: String,
                  test_id: String,
                  select_id: String,
                  loadtime: String,
                  ug_id: String
                )