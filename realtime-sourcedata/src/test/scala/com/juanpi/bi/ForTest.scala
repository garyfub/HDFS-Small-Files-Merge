package com.juanpi.bi

import java.text.{DecimalFormat, SimpleDateFormat}
import java.util.{Calendar, Date}

import org.joda.time.{DateTime, Days, LocalDate, LocalDateTime}

/**
  * Created by gongzi on 2016/9/5.
  */
object DateTool {
  //  1、获取今天日期

  def getNowDate(): String = {
    var now: Date = new Date()
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var hehe = dateFormat.format(now)
    hehe
  }

  //  2、获取昨天的日期
  def getYesterday(): String = {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    var yesterday = dateFormat.format(cal.getTime())
    yesterday
  }

  //    3、获取本周开始日期
  def getNowWeekStart(): String = {
    var period: String = ""
    var cal: Calendar = Calendar.getInstance();
    var df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY)
    //获取本周一的日期
    period = df.format(cal.getTime())
    period
  }

  //    4、获取本周末的时间
  def getNowWeekEnd(): String = {
    var period: String = ""
    var cal: Calendar = Calendar.getInstance();
    var df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    cal.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY); //这种输出的是上个星期周日的日期，因为老外把周日当成第一天
    cal.add(Calendar.WEEK_OF_YEAR, 1) // 增加一个星期，才是我们中国人的本周日的日期
    period = df.format(cal.getTime())
    period
  }

  //    5、本月的第一天
  def getNowMonthStart(): String = {
    var period: String = ""
    var cal: Calendar = Calendar.getInstance();
    var df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    cal.set(Calendar.DATE, 1)
    period = df.format(cal.getTime()) //本月第一天
    period
  }

  //    6、本月的最后一天
  def getNowMonthEnd(): String = {
    var period: String = ""
    var cal: Calendar = Calendar.getInstance();
    var df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    cal.set(Calendar.DATE, 1)
    cal.roll(Calendar.DATE, -1)
    period = df.format(cal.getTime()) //本月最后一天
    period
  }

  //    7、将时间戳转化成日期
  //    时间戳是秒数，需要乘以1000l转化成毫秒
  def DateFormat(time: String): String = {
    var sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var date: String = sdf.format(new Date((time.toLong * 1000l)))
    date
  }

  //    8、时间戳转化为时间，原理同上
  def timeFormat(time: String): String = {
    var sdf: SimpleDateFormat = new SimpleDateFormat("HH:mm:ss")
    var date: String = sdf.format(new Date((time.toLong * 1000l)))
    date
  }

  //    10计算时间差
  //核心工作时间，迟到早退等的的处理
  def getCoreTime(start_time: String, end_Time: String) = {
    var df: SimpleDateFormat = new SimpleDateFormat("HH:mm:ss")
    var begin: Date = df.parse(start_time)
    var end: Date = df.parse(end_Time)
    var between: Long = (end.getTime() - begin.getTime()) / 1000 //转化成秒
    var hour: Float = between.toFloat / 3600
    var decf: DecimalFormat = new DecimalFormat("#.00")
    decf.format(hour) //格式化
  }

  /**
    * Joda-Time主要的特点包括：
    *  1. 易于使用:Calendar让获取"正常的"的日期变得很困难，使它没办法提供简单的方法，而Joda-Time能够 直接进行访问域并且索引值1就是代表January。
    *  2. 易于扩展：JDK支持多日历系统是通过Calendar的子类来实现，这样就显示的非常笨重而且事实 上要实现其它日历系统是很困难的。Joda-Time支持多日历系统是通过基于Chronology类的插件体系来实现。
    *  3. 提供一组完整的功能：它打算提供 所有关系到date-time计算的功能．Joda-Time当前支持8种日历系统，而且在将来还会继续添加，有着比JDK Calendar更好的整体性能等等。
    */
  def JodaDate() = {
     val dateTime: DateTime = new DateTime("2016-08-31");
    val dt = dateTime.plusDays(1)
    println(dt)
  }

  //    测试一下

  def main(args: Array[String]) {

    println("现在时间：" + DateTool.getNowDate())
    println("昨天时间：" + DateTool.getYesterday())
    println("本周开始" + DateTool.getNowWeekStart())
    println("本周结束" + DateTool.getNowWeekEnd())

    println("本月开始" + DateTool.getNowMonthStart())
    println("本月结束" + DateTool.getNowMonthEnd())

    println("\n")

    println(DateTool.timeFormat("1436457603"))
    println(DateTool.DateFormat("1436457603"))

    println(JodaDate())
  }
}
