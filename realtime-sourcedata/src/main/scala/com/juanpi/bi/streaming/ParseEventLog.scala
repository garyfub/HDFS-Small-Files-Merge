//package com.juanpi.bi.streaming
//
//import play.api.libs.json.Json
//
///**
//  * Created by gongzi on 2016/7/01.
//  */
//object ParseEventLog {
//  /* *
//  *
//  *  action id means:
//  *  1  add car
//  *  -1 del from car
//  *  2  buy
//  *  3  collect
//  *  -3 del from collect
//  *  4  view goods action
//  *
//  * */
//  val parseEventLog = (topic: String, line: String) => {
//    val row = Json.parse(line)
//    // todo
//    val llll = .parse(row)
//    val eventType = (row \ "activityname").asOpt[String].getOrElse("")
//
//    val eventId = eventType match {
//      case "click_temai_shoppingbagempty_rejoin" | "click_temai_inpage_skuconfirm" |
//           "click_temai_shoppingbag_rejoin" | "click_temai_inpage_joinbag" | "click_temai_shoppingbag_add" => {
//        1
//      }
//      case "click_temai_shoppingbag_del" | "click_temai_shoppingbag_cut" => {
//        5
//      }
//      case "click_goods_collection" | "click_temai_inpage_collect" | "click_goods_cancel" => {
//        3
//      }
//      case "click_temai_inpage_cancelcollect" => {
//        6
//      }
//      case _ => {
//        0
//      }
//    }
//    (userId, (goodsId, eventId, startTime))
//  }
//}
