package com.juanpi.bi.commonUtils;


import com.google.gson.Gson;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import java.util.Iterator;
import java.util.Set;

public class JsonUtils {

    private final static Gson gson = new Gson();

    public static void print(Object obj) {
        System.out.println(gson.toJson(obj));
    }

    public static void print(String objName, Object obj) {
        System.out.printf("%s: %s", objName, gson.toJson(obj));
        System.out.println();
    }

    public static String toJson(Object obj) {
        return gson.toJson(obj);
    }


    public static Object toBean(JSONObject object, Class beanClass) {
        Object obj = null;
        try {
            Iterator<String> keys=object.keys();
            Object o;
            String key;
            while(keys.hasNext()){
                key=keys.next();
                o=object.get(key);
                if(o instanceof JSONObject){
                    object.put(key, "\""+o.toString()+"\"");
                } else if(o instanceof JSONArray) {
                    object.put(key, "\""+o.toString()+"\"");
                } else {

                }
            }
            obj = JSONObject.toBean(object, beanClass);
        } catch (Exception e) {
            System.out.println(object.toString());
            e.printStackTrace();
        }
        return obj;
    }

    public static Object toBean(JSONObject object, Class beanClass, Set<String> fields) {
        Object obj = null;
        try {
            Iterator<String> keys=object.keys();
            Object o;
            String key;
            while(keys.hasNext()){
                key=keys.next();
                if(fields.contains(key)) {
                    o = object.get(key);
                    if (o instanceof JSONObject) {
                        object.put(key, "\"" + o.toString() + "\"");
                    } else if (o instanceof JSONArray) {
                        object.put(key, "\"" + o.toString() + "\"");
                    } else {

                    }
                }
            }
            obj = JSONObject.toBean(object, beanClass);
        } catch (Exception e) {
            System.out.println(object.toString());
            e.printStackTrace();
        }
        return obj;
    }
}