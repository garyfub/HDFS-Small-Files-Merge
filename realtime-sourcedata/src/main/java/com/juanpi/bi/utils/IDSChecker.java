package com.juanpi.bi.utils;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
/**
 * Created by gongzi on 2016/7/11.
 */
public class IDSChecker {
    private static IDSChecker IDSChecker  = null;
    public static Log log = LogFactory.getLog(IDSChecker.class);

    /**
     * 如果一个类要被声明为static的，只有一种情况，就是静态内部类。如果在外部类声明为static，程序会编译都不会过。在一番调查后个人总结出了3点关于内部类和静态内部类（俗称：内嵌类）
     * 1.静态内部类跟静态方法一样，只能访问静态的成员变量和方法，不能访问非静态的方法和属性，但是普通内部类可以访问任意外部类的成员变量和方法
     * 2.静态内部类可以声明普通成员变量和方法，而普通内部类不能声明static成员变量和方法。
     * 3.静态内部类可以单独初始化
     * 参考：http://www.cnblogs.com/Alex--Yang/p/3386863.html
     */
    public static class Page_Pattern{
        public int id;
        public String pageName;
        public List<String> regex_pattern;

        public boolean match(String url){
            for(String regexp: regex_pattern){
                if(regexp.contains("%")){
                    String[] arr= regexp.split("%");
                    boolean bMatch=true;
                    for(String a:arr){
                        if(a.trim().length()>0 && (!url.contains(a.trim()))){
                            bMatch=false;
                            break;
                        }
                    }
                    if(bMatch){
                        if((!regexp.startsWith("%")) && (!url.startsWith(arr[0]))){
                            bMatch=false;
                        }
                        if((!regexp.endsWith("%")) && (!url.endsWith(arr[arr.length-1]))){
                            bMatch=false;
                        }
                    }
                    if(bMatch){
                        return true;
                    }
                }
                else{ //for regexp
                    if(url.matches(regexp)) return true;
                }
            }
            return false;
        }

        public boolean match(String name ,String extendname){
            if(regex_pattern.size()==1&&name.equals(regex_pattern.get(0))){
                return true;
            }
            if(regex_pattern.size()==2&&name.equals(regex_pattern.get(0))&&extendname.equals(regex_pattern.get(1))){
                return true;
            }
            return false;
        }
    }




    public static IDSChecker getInstance(String s){
        if(IDSChecker == null){
            IDSChecker = new IDSChecker(s);
        }
        return IDSChecker;
    }

    private IDSChecker(String s){
        loadPageID(s);
    }

    public static List<Page_Pattern>  patterns = new ArrayList<Page_Pattern>();

	/*static{
		loadPageID();
	}*/



    /**
     *
     */
    private static List<Page_Pattern> loadPageID(String pathstr) {
        try {
            log.info("Start to loading new page pattern ....");
            InputStream stream =ClassLoader.getSystemClassLoader().getResourceAsStream(pathstr);
            BufferedReader reader;
            reader = new BufferedReader(new InputStreamReader(stream, "UTF-8"));
            String text = null;
            while((text=reader.readLine())!=null){
                String[] arr = text.split(",");
                if(arr.length<3){
                    break;
                }
                Page_Pattern pattern = new Page_Pattern();
                pattern.id = Integer.parseInt(arr[0]);
                pattern.pageName = arr[1];
                pattern.regex_pattern=new ArrayList<String>();
                for(int i=2; i<arr.length;i++){
                    String temp= arr[i].trim();
                    if(temp.length()>0 && !"null".equals(temp)){
                        pattern.regex_pattern.add(temp.toLowerCase());
                    }
                }
                patterns.add(pattern);

            }
            log.info("The new version page pattern has been load successfully.");
        } catch (Exception e) {
            log.info("Failed to load the new page pattern: ", e);
            return null;
        }
        return patterns;
    }

    public static void main(String[] argc){
//		System.out.println(IDSChecker.patterns.get(0).regex_pattern.get(1));

    }





}
