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

  /**
    * 返回日期加上gu_id最后一位，作为log文件的保存目录
    *
    * @param milliseconds
    * @param gu_id
    * @return
    */
  def dateGuidPartitions(milliseconds: Long, gu_id: String): String = {
    val dateTime = new DateTime(milliseconds)
    val date = dateTime.toString("yyyy-MM-dd")
    val gu_hex = (gu_id.last).toLower
    s"date=${date}/gu_hash=${gu_hex}"
  }

  /**
    * 返回 yyyy-MM-dd 格式的日期
    *
    * @param milliseconds
    * @return
    */
  def dateStr(milliseconds: Long): String = {
    val dateTime = new DateTime(milliseconds)
    dateTime.toString("yyyy-MM-dd")
  }

  /**
    * 接受一个时间戳的参数，返回日期和小时
    *
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
    println((gu_id.last).toLower)

    println(dateGuidPartitions("1468929132822".toLong, "13a69d96f245ab71b"))

    val (date, hour) = dateHourStr("1473233633320".toLong)
    println(date)
    println(hour)

    println(dateStr("1474972779928".toLong))
    println(dateStr("1474973422".toLong * 1000))


  }
}
