package com.juanpi.bi.transformer

import org.apache.spark.rdd.RDD

/**
 * Created by juanpi on 2015/8/18.
 */
trait ITransformer extends Serializable{
  def transform(line:String, page:RDD[(Int, (String, String))], event:RDD[(Int, (String, String))]):(String, String)
}
