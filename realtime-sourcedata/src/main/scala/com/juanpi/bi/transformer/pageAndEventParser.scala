package com.juanpi.bi.transformer

/**
  * Created by gongzi on 2016/7/19.
  */
object pageAndEventParser {

  def getTerminalId(os: String): Int =
  {
    os.toLowerCase match {
      case "android" => 2
      case "ios" => 3
      case _ => -999
    }
  }

  def getSiteId(app_name: String): Int =
  {
    app_name.toLowerCase match {
      case "jiu" => 2
      case "zhe" => 1
      case _ => -999
    }
  }

  /**
    * 2016-02-16 加逻辑：IOS直接使用设备号作为gu_id
    * @param jpid
    * @param deviceid
    * @param os
    * @return
    */
  def getGuid(jpid: String, deviceid: String, os: String): String =
  {
    if(os.toLowerCase() == "ios" || jpid.equals("0") || jpid.isEmpty() )
    {
      deviceid
    }
    else jpid
//    os.toLowerCase match {
//      case "android" => jpid
//      case "ios" => deviceid
//      case _ => "0"
//    }

  }

}
