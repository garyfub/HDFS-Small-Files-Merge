package com.juanpi.bi.sc_utils

import com.twitter.algebird.Monoid
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Time
import org.joda.time.DateTime

/**
 * Created by Administrator on 2015/11/16.
 */
object DStreamUtils {
  def addTime[C,T](rdd:RDD[(C,T)],time:Time) = {
    val timestamp = time.milliseconds
    rdd.map(t => (t._1,(timestamp,t._2)))
  }

  /**
   * 过滤掉不同周期的数据
   * @param rdd
   * @param time
   */
  def filterSameCycle[C,T](cycleType:String)(rdd:RDD[(C,(Long,T))], time:Time) = {
    rdd.filter(record =>{
      isSameCycle(time.milliseconds,record._2._1,cycleType)
    })
  }

  def isSameCycle(last:Long,now:Long,cycleType:String):Boolean = {
    val lastDt = new DateTime(last)
    val nowDt = new DateTime(now)

    if (cycleType == "day") {
      val DATE_FORMAT = "yyyy-MM-dd"
      lastDt.toString(DATE_FORMAT).equals(nowDt.toString(DATE_FORMAT))
    } else if (cycleType == "hour") {
      val DATEHOUR_FORMAT = "yyyy-MM-ddHH"
      lastDt.toString(DATEHOUR_FORMAT).equals(nowDt.toString(DATEHOUR_FORMAT))
    } else { // 默认分钟切换
      val DATEHOUR_FORMAT = "yyyy-MM-ddHHmm"
      lastDt.toString(DATEHOUR_FORMAT).equals(nowDt.toString(DATEHOUR_FORMAT))
    }
  }

  def updateTotalCountState[T](cycleType:String)(values:Seq[(Long,T)], state: Option[(Long,T)])(implicit monoid: Monoid[T]):Option[(Long,T)] = {
    val defaultState = (0l,monoid.zero)
    values match {
      case Nil => Some(state.getOrElse(defaultState))
      case _ =>
        val hdT = values(0)._1
        val v = values.map{ case (_, a) => a}.reduce(monoid.plus)
        val stateReceived = state.getOrElse(defaultState)
        if(!isSameCycle(stateReceived._1, hdT,cycleType)) Some((hdT, v)) else Some((hdT, monoid.plus(v,stateReceived._2)))
    }
  }
}
