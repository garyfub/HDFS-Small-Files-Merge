package com.juanpi.bi.sc_utils

import org.joda.time.DateTime

/**
 * Created by juanpi on 2015/9/1.
 */
object TimeUtils {
  def isSameCycle(last:Long,now:Long,cycleType:String):Boolean = {
    val lastDt = new DateTime(last)
    val nowDt = new DateTime(now)

    if(cycleType == "day") {
      val DATE_FORMAT = "yyyy-MM-dd"
      lastDt.toString(DATE_FORMAT).equals(nowDt.toString(DATE_FORMAT))
    }else if(cycleType == "hour"){
      val DATEHOUR_FORMAT = "yyyy-MM-ddHH"
      lastDt.toString(DATEHOUR_FORMAT).equals(nowDt.toString(DATEHOUR_FORMAT))
    }else if(cycleType == "10min"){
      val DATEHOUR_FORMAT = "yyyy-MM-ddHHmm"
      lastDt.toString(DATEHOUR_FORMAT).substring(0,13).equals(nowDt.toString(DATEHOUR_FORMAT).substring(0,13))
    }else {
      false
    }
  }
}
