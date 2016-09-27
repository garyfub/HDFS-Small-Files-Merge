//package com.juanpi.bi.hiveUDF;
//
//import com.juanpi.bi.utils.IDSChecker;
//
//import java.util.List;
//
///**
// * 获取移动端页面类型，类型与数据库dim_page 中定义的ID一致
// *
// * @author qingtian
// *         用法：GetMbPageId(String pageName)
// */
//public class GetMbPageId
//{
//    public static Integer evaluate(final String name, String extendname) {
//
//        if (name == null || name.trim().length() == 0)
//            return -1;
//        if (extendname == null || extendname.trim().length() == 0)
//            extendname = "";
//
//        IDSChecker.getInstance("PageID.properties");
//        List<IDSChecker.Page_Pattern> patterns = IDSChecker.patterns;
//
//        if (patterns == null || patterns.isEmpty()) return -1;
//
//        for (IDSChecker.Page_Pattern pat : patterns) {
//            if (pat.match(name.toLowerCase(), extendname.toLowerCase())) {
//                return pat.id;
//            }
//        }
//        return -1;
//    }
//
//
//    public static void main(String[] argc) {
//        GetMbPageId gh = new GetMbPageId();
//        System.out.println(gh.evaluate("page_tab", "all"));
//    }
//
//}
