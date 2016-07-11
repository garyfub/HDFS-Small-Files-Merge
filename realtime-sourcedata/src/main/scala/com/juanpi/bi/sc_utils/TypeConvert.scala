package com.juanpi.bi.sc_utils

/**
 * Description: TypeConvert
 * Author: jiangnan
 * Update: (2015-08-05 12:07)
 */
object TypeConvert {
  def getSiteId(appName:String):Int = {
    appName match {
      case "zhe" => 1
      case "jiu" => 2
      case _ => -999
    }
  }

  def getTerminalId(os:String):Int = {
    os.toLowerCase match {
      case "android" => 2
      case "ios" => 3
      case "wap" => 4
      case "wx" => 5
      case _ => -999
    }
  }
}
