package com.juanpi.bi.transformer

import scala.collection.mutable

/**
 * Created by juanpi on 2015/8/18.
 */
trait ITransformer extends Serializable{
  def logParser(line:String, dimPage: mutable.HashMap[String, (Int, Int, String, Int)], dimEvent: mutable.HashMap[String, (Int, Int)]):(String, String, Any)
}
