package com.juanpi.bi.utils;

import java.util.List;

/**
 * 获取移动端页面类型，类型与数据库dim_page 中定义的ID一致
 *
 * @author qingtian
 *         用法：GetMbPageId(String pageName)
 */
public class GetMbPageId {

    public static Integer evaluate(final String name, String extendname) {
        //List<Page_Pattern> patterns = new MbPageIDChecker("com/juanpi/hive/props/MbPageID.properties").patterns;
        //IDChecker mc = new IDChecker("com/juanpi/hive/props/MbPageID.properties");
//		String[] a = name.split(", ");
//		String[] b = extendname.split(extendname);
        //a[0]

        if (name == null || name.trim().length() == 0)
            return -1;
        if (extendname == null || extendname.trim().length() == 0)
            extendname = "";
        //IDChecker.getInstance("com/juanpi/bi/props/MbPageID.properties");
        List<IDChecker.Page_Pattern> patterns = IDChecker.patterns;
        if (patterns == null || patterns.isEmpty()) return -1;
        for (IDChecker.Page_Pattern pat : patterns) {
            if (pat.match(name.toLowerCase(), extendname.toLowerCase())) {
                return pat.id;
            }

        }
        return -1;
    }


    public static void main(String[] argc) {
        GetMbPageId gh = new GetMbPageId();
        System.out.println(gh.evaluate("page_center", ""));
    }

}
