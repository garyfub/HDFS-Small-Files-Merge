package com.juanpi.bi.mr_example;

/**
 * Created by gongzi on 2016/8/30.
 */
public class Test {

    public static void main(String[] args) throws  Exception{
//        String value = "00000000-2541-efe3-a880-affd4b2119fa\00130287381\001103489\001\0011465039845338_zhe_1472541289455\0012\0014.1.0\0011\0011\001C2\001河北省\0010\001225_217_326_236_322\0012016-08-30\00115\001250\00114719\001219\001最新折扣\00151506\0010\0014\0011472541640971\0011472541656030\0012500924\0012500924\001\001\001\001\001\0010\001C2\001mb_page\001未知\001111.11.108.112\001\001\001861572038253973\0010\001\001\001\001\001\001\001";
//        final String[] splited = value.split("\001");
//        System.out.println(splited.length);
//        System.out.println(splited[splited.length-1]);

        String key = "123456";
        System.out.println(key.hashCode() & 2147483647);
    }

}
