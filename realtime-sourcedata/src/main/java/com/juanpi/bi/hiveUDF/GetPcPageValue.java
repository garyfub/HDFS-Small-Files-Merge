package com.juanpi.bi.hiveUDF;

import com.juanpi.bi.utils.CateNameChecker;
import com.juanpi.bi.utils.DecodeURLParam;
import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 获取pc端pagevalue
 *
 * @author xiaoqi 用法：GetPcPageValue(url,url_page_id)
 */
public class GetPcPageValue {

    public static String evaluate(String url, String page_id) {
        if (page_id == null || page_id.trim().length() == 0) {
            return null;
        }
        if (url == null || url.trim().length() == 0) {
            return null;
        }

        // CateNameChecker.getInstance("com/juanpi/hive/props/CateName.properties");
        Map<String, CateNameChecker.Page_Pattern> patternsMap = CateNameChecker.patternsMap;
        if (patternsMap == null || patternsMap.isEmpty())
            return null;

        if (!patternsMap.containsKey(page_id)) {
            return url.toLowerCase();
        }

        CateNameChecker.Page_Pattern pat = patternsMap.get(page_id);
        if ("6".equals(pat.page_type_id)) {
            try {
                String result = null;
                String res = null;
                if (url == null) {
                    return result;
                }
                DecodeURLParam d = new DecodeURLParam();
                String urlString = url.trim().replace("_", "--").toLowerCase();
                Pattern pattern = Pattern
                        .compile("keywords=([\u4E00-\u9FA5\\s0-9A-Za-z%+-]+)");
                Matcher matcher = pattern.matcher(urlString);
                Pattern pattern1 = Pattern
                        .compile("keyword=([\u4E00-\u9FA5\\s0-9A-Za-z%+-]+)");
                Matcher matcher1 = pattern1.matcher(urlString);
                if (matcher.find()) {
                    res = matcher.group(1).replace("--", "_");
                    result = d.evaluate(res);
                } else if (matcher1.find()) {
                    res = matcher1.group(1).replace("--", "_");
                    result = d.evaluate(res);
                }
                if (result.indexOf("\\") != -1 || result.indexOf("%") != -1) {
                    return null;
                }
                return result;
            } catch (Exception e) {
                return null;
            }
        } else if ("3".equals(pat.page_type_id) || "7".equals(pat.page_type_id) || "8".equals(pat.page_type_id) || "9".equals(pat.page_type_id)) {
            String result = "";
            if (url == null)
                url = "";
            url = url.toLowerCase();
            Pattern pattern = Pattern.compile("(deal|jump|shop|click|click/auth/)?/?(\\d+)");
            Matcher matcher = pattern.matcher(url);
            if (matcher.find()) {
                result = matcher.group(2);
            }

            result = decodeGoodid(result);
            return result;
        } else if (pat.page_type_id.equals("5")) {
            if (url == null || url.trim().length() == 0) {
                return null;
            }
            Pattern p = Pattern.compile("(zhuanti|act|event)/([a-zA-Z0-9]+)");
            Matcher m = p.matcher(url);
            if (m.find()) {
                return m.group().toLowerCase();
            }
            return null;
        } else if ("4".equals(pat.page_type_id)) {
            return pat.page_value;
        } else {
            return url.toLowerCase();
        }

    }

    private static StringBuilder sb = new StringBuilder();

    public static String decodeGoodid(String goodid) {

        try {
            if (goodid.trim().equals(StringUtils.EMPTY)) {
                return goodid;
            }
            int l = goodid.length();
            Map<Integer, String> tmpstr = new HashMap<Integer, String>();
            int flag = 1;
            int c = 0;
            for (int i = 0; i < l; i++) {
                if (i != 0 && i % 2 == 0) {
                    flag = -flag;
                    if (flag == 1) {
                        c++;
                    }
                }
                if (i == l - 1) {
                    for (int j = 0; j < l; j++) {
                        if (tmpstr.get(j) == null) {
                            tmpstr.put(j, String.valueOf(goodid.charAt(i)));
                        }
                    }
                } else {
                    if (i % 2 == 0) {
                        if (flag == 1) {
                            tmpstr.put(((int) i / 2) - c,
                                    String.valueOf(goodid.charAt(i)));
                        } else {
                            tmpstr.put(((int) l / 2) + c,
                                    String.valueOf(goodid.charAt(i)));
                        }
                    } else {
                        if (flag == 1) {
                            tmpstr.put(l - (int) ((i - c * 2) / 2) - 1,
                                    String.valueOf(goodid.charAt(i)));
                        } else {
                            tmpstr.put(((int) (l / 2)) - 1 - c,
                                    String.valueOf(goodid.charAt(i)));
                        }
                    }
                }
            }
            TreeMap<Integer, String> treemap = new TreeMap<Integer, String>(
                    tmpstr);
            sb.setLength(0);
            for (Entry<Integer, String> e : treemap.entrySet()) {
                sb.append(e.getValue());
            }
            goodid = String.valueOf(Long.valueOf(sb.toString()) / 7 - 201341);
        } catch (Exception e) {
            goodid = StringUtils.EMPTY;
        }
        return goodid;
    }

    public static void main(String[] argc) {
        GetPcPageValue gh = new GetPcPageValue();
        String url = "http://www.juanpi.com/zhuanti/shop618";
        String page_id = "34";
        System.out.println(gh.evaluate(url, page_id));
    }

}
