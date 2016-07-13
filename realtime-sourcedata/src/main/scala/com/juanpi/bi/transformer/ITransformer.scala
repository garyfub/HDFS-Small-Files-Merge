package com.juanpi.bi.transformer

/**
 * Created by juanpi on 2015/8/18.
 */
trait ITransformer extends Serializable{
  def transform(line:String):(String, String)
}
