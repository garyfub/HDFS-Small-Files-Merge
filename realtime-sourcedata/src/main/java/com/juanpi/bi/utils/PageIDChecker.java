package com.juanpi.bi.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @author zhuokun
 */
public class PageIDChecker {

    public static Log log = LogFactory.getLog(PageIDChecker.class);

    public static class Page_Pattern {
        public int id;
        public String pageName;
        public List<String> regex_pattern;

        public boolean match(String url) {
            for (String regexp : regex_pattern) {
                if (regexp.contains("%")) {
                    String[] arr = regexp.split("%");
                    boolean bMatch = true;
                    for (String a : arr) {
                        if (a.trim().length() > 0 && (!url.contains(a.trim()))) {
                            bMatch = false;
                            break;
                        }
                    }
                    if (bMatch) {
                        if ((!regexp.startsWith("%")) && (!url.startsWith(arr[0]))) {
                            bMatch = false;
                        }
                        if ((!regexp.endsWith("%")) && (!url.endsWith(arr[arr.length - 1]))) {
                            bMatch = false;
                        }
                    }
                    if (bMatch) {
                        return true;
                    }
                } else { //for regexp
                    if (url.matches(regexp)) return true;
                }
            }
            return false;
        }

    }

    public static List<Page_Pattern> patterns = new ArrayList<Page_Pattern>();

    static {
        loadPageID();
    }

    public static void loadPageID() {

        try {
            log.info("Start to loading new page pattern ....");
            //InputStream stream = ClassLoader.getSystemClassLoader().getResourceAsStream("com/juanpi/bi/props/PageID.properties");
            InputStream stream = PageIDChecker.class.getClassLoader().getResourceAsStream("PageID.properties");
            BufferedReader reader;
            reader = new BufferedReader(new InputStreamReader(stream, "UTF-8"));
            String text = null;
            while ((text = reader.readLine()) != null) {

                String[] arr = text.split(",");
                if (arr.length < 4) {
                    break;
                }
                Page_Pattern pattern = new Page_Pattern();
                pattern.id = Integer.parseInt(arr[0]);
                pattern.pageName = arr[1];
                pattern.regex_pattern = new ArrayList<String>();
                for (int i = 2; i < arr.length; i++) {
                    String temp = arr[i].trim();
                    if (temp.length() > 0 && !"null".equals(temp)) {
                        pattern.regex_pattern.add(temp.toLowerCase());
                        //	System.out.println(temp);
                    }
                }
                patterns.add(pattern);

            }
            log.info("The new version page pattern has been load successfully.");
        } catch (Exception e) {
            log.info("Failed to load the new page pattern: ", e);
        }
    }

    public static void main(String[] argc) {
        System.out.println(PageIDChecker.patterns.get(0).regex_pattern.get(1));

    }


}
