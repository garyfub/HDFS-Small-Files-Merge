package com.juanpi.bi.test

/**
  * Created by gongzi on 2016/8/10.
  */
case class Page1(
                 // page 独有属性
                 source: String,
                 ip: String
               )

/**
  * 用户访问产生的点击
  *
  * @param event_id
  * @param event_value
  */
case class Event1(
                  // event 独有属性
                  event_id: String,
                  event_value: String
                )

object TestCaseClass {

  def combine(xss: Product*) = xss.toList.flatten(_.productIterator)

  def main(args: Array[String]) {
//    println(Page1.unapply(p1).get)
//    val plist = Page1.unapply(p1).get
//    println(plist.productIterator.toList)
//    val comb = combine(p1, e1)
//    println(comb.mkString("#"))
  }
}