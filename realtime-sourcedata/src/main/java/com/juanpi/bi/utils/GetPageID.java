package com.juanpi.bi.utils;

/**
 * 获取页面类型，类型与数据库dim_page_type 中定义的Page_ID 一致，特殊返回值介绍：-1表示站内未知页，-999表示站外页，非法URL或空URL时返回null。
 */

public class GetPageID {

    /**
     * 获取指定url 对应的一级页面类型
     *
     * @param s
     * @return -1表示内部未知页，-999表示外部页
     */
    public static Integer evaluate(final String s) {
        return PageTypeUtils.getPageID(s);
    }


    public static void main(String[] argc) {
        GetPageID gh = new GetPageID();
        String url = "http://shop.juanpi.com/deal/1178810";
        System.out.println(gh.evaluate(url));
    }
}
