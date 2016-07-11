package com.juanpi.bi.streaming

/**
 * Created by juanpi on 2015/8/18.
 */
package object streaming {

}

case class DateHour(date:String,hour:String){

  override def toString = s"date=${date}/hour=${hour}"

  def toDateString = s"date=${date}"
}