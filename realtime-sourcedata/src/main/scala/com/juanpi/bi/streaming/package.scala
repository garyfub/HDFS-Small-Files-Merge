package com.juanpi.bi.streaming

package object streaming {

}

case class DateHour(date:String,hour:String){

  override def toString = s"date=${date}/hour=${hour}"

  def toDateString = s"date=${date}"
}