package com.juanpi.bi

import play.api.libs.json.{JsValue, Json}

/**
  * Created by gongzi on 2016/12/30.
  */
object ParsrJsonArray {


  def parse(): Unit = {
    val line = "[{\"packagename\":\"com.meizu.net.pedometer\",\"firstinstall\":1477582203000,\"lastupdate\":1482880862687},{\"packagename\":\"com.juanpi.ui\",\"firstinstall\":1458698465961,\"lastupdate\":1481971592627},{\"packagename\":\"com.mediatek.fwk.plugin\",\"firstinstall\":1262304246000,\"lastupdate\":1478246542354},{\"packagename\":\"com.happyelements.AndroidAnimal.qq\",\"firstinstall\":1458718012779,\"lastupdate\":1481841138330}],{\"packagename\":\"com.sohu.inputmethod.sogou\",\"firstinstall\":1458811171164,\"lastupdate\":1474350546982},{\"packagename\":\"com.tencent.qqmusic\",\"firstinstall\":1458698481047,\"lastupdate\":1482226693678},{\"packagename\":\"com.meizu.account.pay\",\"firstinstall\":1464578839000,\"lastupdate\":1478907002329},{\"packagename\":\"com.android.browser\",\"firstinstall\":1445538441000,\"lastupdate\":1482204282396},{\"packagename\":\"com.tencent.reading\",\"firstinstall\":1479044403939,\"lastupdate\":1482388078711},{\"packagename\":\"com.tencent.mm\",\"firstinstall\":1458696449503,\"lastupdate\":1481822513526},{\"packagename\":\"com.meizu.flyme.update\",\"firstinstall\":1445538441000,\"lastupdate\":1479595494821},{\"packagename\":\"com.meizu.net.map\",\"firstinstall\":1464578839000,\"lastupdate\":1482723280885},{\"packagename\":\"com.chinamworld.bocmbci\",\"firstinstall\":1461982360683,\"lastupdate\":1482469032184},{\"packagename\":\"com.meizu.flyme.wallet\",\"firstinstall\":1477582203000,\"lastupdate\":1478825874581},{\"packagename\":\"com.husor.beibei\",\"firstinstall\":1458711139440,\"lastupdate\":1482204221651},{\"packagename\":\"com.meizu.media.reader\",\"firstinstall\":1445538441000,\"lastupdate\":1481882241058},{\"packagename\":\"com.tencent.qqpimsecure\",\"firstinstall\":1479045127423,\"lastupdate\":1481710285113},{\"packagename\":\"com.xunmeng.pinduoduo\",\"firstinstall\":1482842611448,\"lastupdate\":1482842611448},{\"packagename\":\"com.UCMobile\",\"firstinstall\":1458698420378,\"lastupdate\":1482296442570},{\"packagename\":\"com.tencent.mobileqq\",\"firstinstall\":1458696392691,\"lastupdate\":1481920405261},{\"packagename\":\"com.meizu.media.ebook\",\"firstinstall\":1464578839000,\"lastupdate\":1478293830181},{\"packagename\":\"com.meizu.media.music\",\"firstinstall\":1445538441000,\"lastupdate\":1480597887378},{\"packagename\":\"com.meizu.media.gallery\",\"firstinstall\":1445538441000,\"lastupdate\":1480068463214},{\"packagename\":\"com.flyme.netadmin\",\"firstinstall\":1464578839000,\"lastupdate\":1479950967989},{\"packagename\":\"com.pingan.carowner\",\"firstinstall\":1481592297806,\"lastupdate\":1482296475170},{\"packagename\":\"com.meizu.voiceassistant\",\"firstinstall\":1445538441000,\"lastupdate\":1481195280173},{\"packagename\":\"com.youku.phone.player.meizu\",\"firstinstall\":1476460958608,\"lastupdate\":1476460958608},{\"packagename\":\"com.baidu.map.location\",\"firstinstall\":1464578839000,\"lastupdate\":1464578839000},{\"packagename\":\"com.jingdong.app.mall\",\"firstinstall\":1458807242785,\"lastupdate\":1482461795891},{\"packagename\":\"com.tmall.wireless\",\"firstinstall\":1458698546665,\"lastupdate\":1481920617490},{\"packagename\":\"com.autonavi.minimap\",\"firstinstall\":1474036676934,\"lastupdate\":1480545016831},{\"packagename\":\"com.taobao.taobao\",\"firstinstall\":1458698576158,\"lastupdate\":1480115931798},{\"packagename\":\"com.qihoo.dr\",\"firstinstall\":1476406335226,\"lastupdate\":1481100101856},{\"packagename\":\"com.meizu.flyme.weather\",\"firstinstall\":1464578839000,\"lastupdate\":1482137047858},{\"packagename\":\"com.eg.android.AlipayGphone\",\"firstinstall\":1458811367300,\"lastupdate\":1482461741096}]"
    val row = Json.parse(line)
//    val subCategories = (row \ "sub-categories").as[List[Map[String, String]]]
    val res = row.as[List[Map[String, String]]]
//    println(subCategories)
//    val names1 = subCategories.map(_("name"))
    val names = res.map(_("packagename"))

    println(names)
  }

  def main(args: Array[String]): Unit = {
    parse()
  }

}
