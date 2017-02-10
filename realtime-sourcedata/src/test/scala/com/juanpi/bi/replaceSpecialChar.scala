package com.juanpi.bi

/**
  * 替换段落中的多个空格、换行、制表符
  * Created by gongzi on 2017/2/10.
  */
object replaceSpecialChar {

  def replaceChars(str: String): String = {
    val res = str.replaceAll("(\0|\\s*|\r|\n)", "")
    res
  }

  /**
    * Intellij 中 scala main的快捷键 main
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val str = """品牌：秋壳	货号：4Q201	厚薄：常规
                衣门襟：其他	组合形式：单件	颜色：黑色
                流行元素：其他	服装版型：直筒	衣长：常规款
                领型：圆领	风格：通勤	尺码：S,M,L,XS
                面料材质：聚酯纤维	年份季节：2017年春季"""
    val res = replaceChars(str)
    println(res)
  }
}
