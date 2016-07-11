//package com.juanpi.bi.utils;
//
//import java.util.HashMap;
//import java.util.Map;
//
///**
// * Created by gongzi on 2016/7/8.
// */
//public class ReadMysql {
//
//    public static Map<Long, Map<String, Object>> map = new HashMap<>();
//
//    static {
//
//        init();
//
//        Thread t = new Thread(new Runnable() {
//            @Override
//            public void run() {
//                while (true) {
//                    try {
//                        Thread.sleep(1000 * 60 * 5);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//
//                    init();
//                }
//            }
//        });
//        t.start();
//
//    }
//
//    private static Map<Long, Map<String, Object>> init() {
//        //读取mysql
//        Map<Long, Map<String, Object>> tempMap = new HashMap<>();
//        //
//        map = tempMap;
//
//        return map;
//    }
//
//
//}
