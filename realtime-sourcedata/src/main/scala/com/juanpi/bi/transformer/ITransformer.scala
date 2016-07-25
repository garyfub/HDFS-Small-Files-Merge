package com.juanpi.bi.transformer

import scala.collection.mutable

/**
 * Created by juanpi on 2015/8/18.
 */
trait ITransformer extends Serializable{
  def transform(line:String, dimpage: mutable.HashMap[String, (Int, Int, String, Int)]):(String, Any)
}
