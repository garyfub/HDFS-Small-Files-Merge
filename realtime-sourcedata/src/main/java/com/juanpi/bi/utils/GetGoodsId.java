package com.juanpi.bi.utils;

import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * 获取页面参数， pagevalue的值是goodid 。
 *
 * @author qingtian
 *         用法：getGoodsId(String url)
 */

public class GetGoodsId {

    private static StringBuilder sb = new StringBuilder();

    public static String evaluate(String s) {
        String result = "";

        if (s == null) s = "";

        s = s.toLowerCase();
        Pattern pattern = Pattern.compile("(deal|jump|shop|click|click/auth/)?/?(\\d+)");
        Matcher matcher = pattern.matcher(s);
        if (matcher.find()) {
            result = matcher.group(2);
        }

        result = decodeGoodid(result);
        return result;
    }

    public static String evaluate(String s, Integer id) {
        return evaluate(s);
    }

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
                            tmpstr.put(((int) i / 2) - c, String.valueOf(goodid.charAt(i)));
                        } else {
                            tmpstr.put(((int) l / 2) + c, String.valueOf(goodid.charAt(i)));
                        }
                    } else {
                        if (flag == 1) {
                            tmpstr.put(l - (int) ((i - c * 2) / 2) - 1, String.valueOf(goodid.charAt(i)));
                        } else {
                            tmpstr.put(((int) (l / 2)) - 1 - c, String.valueOf(goodid.charAt(i)));
                        }
                    }
                }
            }

            TreeMap<Integer, String> treemap = new TreeMap<Integer, String>(tmpstr);
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

    }
}
