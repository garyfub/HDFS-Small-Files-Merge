package com.juanpi.bi.sc_utils

import com.juanpi.bi.streaming.DateHour
import org.joda.time.DateTime

/**
 * Created by juanpi on 2015/8/18.
 */
object DateUtils {
  def dateHour(milliseconds: Long):DateHour = {
    val dateTime = new DateTime(milliseconds)
    DateHour(dateTime.toString("yyyy-MM-dd"), dateTime.toString("H"))
  }

  def dateGuid(milliseconds: Long, gu_id: String): String = {
    val dateTime = new DateTime(milliseconds)
    val date = dateTime.toString("yyyy-MM-dd")
    val gu_hash = dateTime.toString("H")
    s"date=${date}/hour=${gu_hash}"
  }

  /**
    * 接受一个时间戳的参数，返回日期和小时
    * @param milliseconds
    * @return
    */
  def dateHourStr(milliseconds: Long): (String, String) = {
    val dateTime = new DateTime(milliseconds)
    (dateTime.toString("yyyy-MM-dd"), dateTime.toString("H"))
  }

  def md5Hash(text: String) : String = {
    java.security.MessageDigest.getInstance("MD5").digest(text.getBytes()).map(0xFF & _).map { "%02x".format(_) }.foldLeft(""){_ + _}
  }

  def main(args: Array[String]) {
    val gu_id = "ffffffff-bc21-7da8-ffff-ffffe4de7969"
    println(gu_id.substring(24))
    println(md5Hash(gu_id))
//    dateGuid("1468929132822".toLong, ("ffffffff-bc21-7da8-ffff-ffffe4de7969").hashCode())
  }
}
