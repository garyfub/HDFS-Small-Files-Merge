package com.juanpi.bi.sc_utils

import com.juanpi.bi.streaming.DateHour
import org.joda.time.DateTime

/**
 * Created by juanpi on 2015/8/18.
 */
object DateUtils {
  def dateHour(milliseconds:Long):DateHour = {
    val dateTime = new DateTime(milliseconds)
    DateHour(dateTime.toString("yyyy-MM-dd"),dateTime.toString("H"))
  }

}
