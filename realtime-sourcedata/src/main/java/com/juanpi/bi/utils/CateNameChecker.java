package com.juanpi.bi.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CateNameChecker {
    private static CateNameChecker catenameChecker = null;
    public static Log log = LogFactory.getLog(CateNameChecker.class);

    public static class Page_Pattern {
        public String page_id;
        public String page_value;
        public String page_type_id;

        public boolean match(String id) {
            if (id.equals(page_id))
                return true;
            else
                return false;
        }

    }

    /*	public static CateNameChecker getInstance(String s){
            if(catenameChecker == null){
                catenameChecker = new CateNameChecker(s);
            }
            return catenameChecker;
        }

        private CateNameChecker(String s){
            loadCateName(s);
            loadCateNameMap(s);
        }
            */
    public static List<Page_Pattern> patterns = new ArrayList<Page_Pattern>();
    public static Map<String, Page_Pattern> patternsMap = new HashMap<String, Page_Pattern>();

    static {
        loadCateNameMap();
    }


    /**
     *
     */
    private static List<Page_Pattern> loadCateName() {
        try {
            log.info("Start to loading new page pattern ....");
            //	InputStream stream =ClassLoader.getSystemClassLoader().getResourceAsStream(pathstr);
            InputStream stream = CateNameChecker.class.getClassLoader().getResourceAsStream("CateName.properties");
            BufferedReader reader;
            reader = new BufferedReader(new InputStreamReader(stream, "GBK"));
            String text = null;
            while ((text = reader.readLine()) != null) {
                String[] arr = text.split(",");
                if (arr.length < 3) {
                    break;
                }
                Page_Pattern pattern = new Page_Pattern();
                pattern.page_id = arr[0];
                pattern.page_value = arr[1];
                pattern.page_type_id = arr[2];
                patterns.add(pattern);

            }
            log.info("The new version page pattern has been load successfully.");
        } catch (Exception e) {
            log.info("Failed to load the new page pattern: ", e);
            return null;
        }
        return patterns;
    }


    private static Map<String, Page_Pattern> loadCateNameMap() {
        try {
            log.info("Start to loading new page pattern ....");
            // InputStream stream =ClassLoader.getSystemClassLoader().getResourceAsStream(pathstr);
            InputStream stream = CateNameChecker.class.getClassLoader().getResourceAsStream("CateName.properties");
            BufferedReader reader;
            reader = new BufferedReader(new InputStreamReader(stream, "GBK"));
            String text = null;
            while ((text = reader.readLine()) != null) {
                String[] arr = text.split(",");
                if (arr.length < 3) {
                    break;
                }
                Page_Pattern pattern = new Page_Pattern();
                pattern.page_id = arr[0];
                pattern.page_value = arr[1];
                pattern.page_type_id = arr[2];
                patternsMap.put(arr[0], pattern);

            }
            log.info("The new version page pattern has been load successfully.");
        } catch (Exception e) {
            log.info("Failed to load the new page pattern: ", e);
            return null;
        }
        return patternsMap;
    }

    public static void main(String[] argc) {
//		System.out.println(IDChecker.patterns.get(0).regex_pattern.get(1));

    }


}