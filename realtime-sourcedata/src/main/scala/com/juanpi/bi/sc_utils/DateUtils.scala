package com.juanpi.bi.sc_utils

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

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

  /**
    *
    * @return 返回当前的日期串
    */
  def getDateNow():String={
    val dateTime = new DateTime()
    val dt = dateTime.toString("yyyy-MM-dd")
    dt
  }

  /**
    * 指定日期和间隔天数，返回指定日期前N天的日期 date - N days
    * @param dt
    * @param interval
    * @return
    */
  def getDaysBefore(dt: Date, interval: Int):String = {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")

    val cal: Calendar = Calendar.getInstance()
    cal.setTime(dt);

    cal.add(Calendar.DATE, - interval)
    val yesterday = dateFormat.format(cal.getTime())
    yesterday
  }


  /**
    * 指定日期和间隔天数，返回指定日期前N天的日期： date + N days
    * @param dt
    * @param interval
    * @return
    */
  def getDaysLater(dt: Date, interval: Int):String = {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")

    val cal: Calendar = Calendar.getInstance()
    cal.setTime(dt);

    cal.add(Calendar.DATE, + interval)
    val yesterday = dateFormat.format(cal.getTime())
    yesterday
  }

  /**
    * 2017-01-17  A Week Ago is 2017-01-07
    * @return
    */
  def getWeekAgoDateStr(): String = {
    val dt: Date = new Date()
    val dtStr = getDaysBefore(dt, 7)
    dtStr
  }

  /**
    *  2017-01-17 A Week Later is  2017-01-21
    * @return
    */
  def getWeekLaterDateStr(): String = {
    val dt: Date = new Date()
    val dtStr = getDaysLater(dt, 7)
    dtStr
  }

  def getYesterday(): String = {
    // Calendar.DATE
    val dt: Date = new Date()
    val yesterday = getDaysBefore(dt, 1)
    return yesterday
  }

  def getNowWeekStart():String={
    var period:String=""
    var cal:Calendar =Calendar.getInstance();
    var df:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY)
    //获取本周一的日期
    period=df.format(cal.getTime())
    period
  }

  def getNowWeekEnd():String={
    var period:String=""
    var cal:Calendar =Calendar.getInstance();
    var df:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    //这种输出的是上个星期周日的日期，因为老外把周日当成第一天
    cal.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
    // 增加一个星期，才是我们中国人的本周日的日期
    cal.add(Calendar.WEEK_OF_YEAR, 1)
    period=df.format(cal.getTime())
    period
  }


  def getNowMonthStart():String={
    var period:String=""
    var cal:Calendar =Calendar.getInstance();
    var df:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    cal.set(Calendar.DATE, 1)
    period=df.format(cal.getTime())//本月第一天
    period
  }

  def getNowMonthEnd():String={
    var period:String=""
    var cal:Calendar =Calendar.getInstance();
    var df:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    cal.set(Calendar.DATE, 1)
    cal.roll(Calendar.DATE,-1)
    period=df.format(cal.getTime())//本月最后一天
    period
  }

  /**
    * 将时间戳转化成日期
    * @param time
    * @return
    */
  def DateFormat(time:String):String={
    var sdf:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var date:String = sdf.format(new Date((time.toLong*1000l)))
    date
  }

  /**
    * 时间戳转化为时间
    * @param time
    * @return
    */
  def timeFormat(time:String):String={
    var sdf:SimpleDateFormat = new SimpleDateFormat("HH:mm:ss")
    var date:String = sdf.format(new Date((time.toLong*1000l)))
    date
  }

  def main(args: Array[String]) {
    val d1 = getWeekAgoDateStr
    val d7 = getWeekLaterDateStr
    if(d7 > d1) {
      println("1111111")
    }
  }
}
