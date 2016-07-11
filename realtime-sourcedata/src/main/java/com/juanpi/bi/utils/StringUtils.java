package com.juanpi.bi.utils;

import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;

/**
 * @author zhuokun
 */
public class StringUtils {

    public static String multipleSplit(final String s, String... vars) {
        if (s == null || s.trim().length() == 0) {
            return null;
        }
        String dest = s.trim();
        for (String var : vars) {
            if (var == null || var.trim().length() == 0) {
                break;
            }
            int pos = 0;
            String[] parse = var.trim().split("~");
            if (parse.length > 2 || parse.length < 1) {
                return null;
            }
            if (parse.length == 2) {
                pos = Integer.parseInt(parse[1]);
            }
            String[] arr = dest.split(parse[0]);
            if (arr.length > pos) {
                dest = dest.split(parse[0])[pos];
            } else {
                return null;
            }
        }
        return dest;
    }

    public static String recursiveDecode(String code) {
        String temp = null;
        String res = code;
        try {
            do {
                temp = res;
                res = URLDecoder.decode(temp, "UTF-8");
            } while (!temp.equals(res));
        } catch (Exception ex) {
        }
        return res;
    }

    public static boolean isNumeric(String str) {
        return str.matches("^[0-9]+$");
    }

    public static boolean isDescriable(String str) {
        if (str == null || str.trim().length() == 0) return false;
        String temp = str.trim();
        for (int i = 0; i < temp.length(); i++) {
            if (temp.charAt(i) <= 'z' && temp.charAt(i) >= 'a') return true;
            if (temp.charAt(i) <= 'Z' && temp.charAt(i) >= 'A') return true;
            if (temp.charAt(i) <= '\u9fa5' && temp.charAt(i) >= '\u4e00') return true;
        }
        return false;
    }

    public static List<String> split(String s, String rep) {
        if (s == null || s.trim().length() == 0 || "null".equalsIgnoreCase(s.trim())) {
            return null;
        }
        String[] arr = s.trim().split(rep);
        List<String> res = new ArrayList<String>();
        for (String str : arr) {
            if (str != null && str.trim().length() > 0) {
                res.add(str.trim());
            }
        }
        return res;
    }

    public static String getSlice(String s, String sep, int get) {
        if (s == null) {
            return null;
        }
        String[] arr = s.toString().split(sep.toString());
        int index = Math.abs(get);
        if (index > arr.length) {
            return null;
        }
        if (get < 0) {
            return arr[arr.length + get];
        }
        return arr[get - 1];
    }

    public static String getSlice(String s, String sep, String sep_new, int from, int to) {
        if (s == null) {
            return null;
        }
        String[] arr = s.toString().split(sep.toString());

        int f = 0, t = 0;
        if (from < 0) {
            f = arr.length + from;
        } else {
            f = from - 1;
        }
        if (to < 0) {
            t = arr.length + to;
        } else {
            t = to - 1;
        }

        if (f > t || f >= arr.length || t < 0) {
            return null;
        }
        StringBuilder sb = new StringBuilder(arr[f]);
        for (int i = f + 1; i <= Math.min(t, arr.length - 1); i++) {
            sb.append(sep_new + arr[i]);
        }
        return sb.toString();
    }

    public static String getSlice(String s, String sep, int from, int to) {
        return getSlice(s, sep, sep, from, to);
    }

    public static String extractChineseWord(String s) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            if (isChineseChar(s.charAt(i))) {
                sb.append(s.charAt(i));
            }
        }
        return sb.toString();
    }

    public static boolean isChineseChar(char c) {
        return c >= '一' && c <= '龟';
    }

    public static void main(String[] argc) throws Exception {
        System.out.println(recursiveDecode("%E7%AC%94%E8%AE%B0%E6%9C%AC"));
        System.out.println(recursiveDecode("%25E5%25A4%259A%25E8%258A%25AC"));
        System.out.println(recursiveDecode("多芬"));
        System.out.println(isNumeric("00013223"));
        System.out.println(isNumeric("0001322a"));

        System.out.println(isDescriable("00013223"));
        System.out.println(isDescriable("0001322a"));
        System.out.println(isDescriable("  ^ +-*、 &"));
        System.out.println(isDescriable("^中——"));
    }
}
