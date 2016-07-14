package com.juanpi.bi.hiveUDF;


import com.juanpi.bi.utils.CateNameChecker;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 获取mbpagevalue
 *
 * @author xiaoqi 用法：GetMbPageValue(extend_params,page_id)
 */
public class GetMbPageValue {

    public static String evaluate(String extend_params, String page_id) {
        if (page_id == null || page_id.trim().length() == 0) {
            return null;
        }
        if (extend_params == null || extend_params.trim().length() == 0) {
            return null;
        }
        // CateNameChecker.getInstance("com/juanpi/hive/props/CateName.properties");
        Map<String, CateNameChecker.Page_Pattern> patternsMap = CateNameChecker.patternsMap;
        if (patternsMap == null || patternsMap.isEmpty())
            return null;

        if (!patternsMap.containsKey(page_id)) {
            return extend_params.toLowerCase();
        }

        CateNameChecker.Page_Pattern pat = patternsMap.get(page_id);

        if ("5".equals(pat.page_type_id)) {
            if (extend_params == null || extend_params.trim().length() == 0) {
                return null;
            }
            Pattern p = Pattern.compile("(zhuanti|act|event)/([a-zA-Z0-9]+)");
            Matcher m = p.matcher(extend_params);
            if (m.find()) {
                return m.group().toLowerCase();
            }
            return null;
        } else if ("4".equals(pat.page_type_id)) {

            return pat.page_value;

        } else {
            return extend_params.toLowerCase();
        }

    }

    public static void main(String[] argc) {
        GetMbPageValue gh = new GetMbPageValue();
        String extend_params = "page_temai_goods";
        String page_id = "154";
        System.out.println(gh.evaluate(extend_params, page_id));
    }
}