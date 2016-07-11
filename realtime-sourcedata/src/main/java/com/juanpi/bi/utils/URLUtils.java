package com.juanpi.bi.utils;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhuokun
 */

public class URLUtils {

    public static List<String> country = new ArrayList<String>();

    public static final String IP_REGEX = "([1-9]|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])(\\.(\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])){3}";

    static {
        country.add("cn");
        country.add("hk");
        country.add("us");
    }

    @Deprecated
    public static Long getProductID(String url) {
        if (url == null || url.trim().length() == 0) {
            return null;
        }
        String str = url.trim();
        if (str.matches("^http://www.yihaodian.com/product/[0-9]+_[0-9]+.*$")
                || str.matches("^http://www.yihaodian.com/product/[0-9]+$")
                || str.matches("^http://www.1mall.com/product/[0-9]+_[0-9]+.*$")
                || str.matches("^http://www.1mall.com/product/[0-9]+$")) {
            str = StringUtils.multipleSplit(str, "/product/~1", "_");
        } else if (str.matches("^http://www.yihaodian.com/product/detail.do[?]productID=.*$")
                || str.matches("^http://www.1mall.com/product/detail.do[?]productID=.*$")) {
            if (str.contains("productID=?")) {
                return null;
            }
            str = getUrlParam(str, "productID");
        } else if (str.matches("^http://www.yihaodian.com/product/[0-9]+[?].*$")
                || str.matches("^http://www.1mall.com/product/[0-9]+[?].*$")) {
            str = StringUtils.multipleSplit(str, "/product/~1", "[?]");
        } else {
            return null;
        }
        try {
            return Long.valueOf(str);
        } catch (Exception ex) {
            //Invalid ProductID, use default 0 instead.
            return null;
        }
    }

    @Deprecated
    public static Long getUrlCategory(String url) {
        if (url == null || url.trim().length() == 0) {
            return null;
        }
        String temp = url.trim();

        if (temp.startsWith("http://search.yihaodian.com/s/c")) {
            temp = temp.substring("http://search.yihaodian.com/s/c".length());
        } else if (temp.startsWith("http://www.yihaodian.com/ctg/s2/c")) {
            temp = temp.substring("http://www.yihaodian.com/ctg/s2/c".length());
        } else if (temp.startsWith("http://www.yihaodian.com/ctg/s/c")) {
            temp = temp.substring("http://www.yihaodian.com/ctg/s/c".length());
        } else {
            return null;
        }

        String res = StringUtils.multipleSplit(temp, "-", "[?]");
        if (res == null || res.trim().length() == 0) {
            return 0L;
        }
        Long val = null;

        try {
            val = Long.parseLong(res);
        } catch (Exception ex) {

        }
        return val;
    }

    public static String getUrlParam(final String url, String param) {
        if (url == null || url.trim().length() == 0
                || param == null || param.trim().length() == 0) {
            return null;
        }

        String temp = url.trim();
        param = param.trim();
        String[] prefix = {"&", "?", "#"};
        boolean include = false;
        for (String p : prefix) {
            int index = temp.indexOf(p + param + "=");
            if (index != -1) {
                temp = temp.substring(index + 2 + param.length());
                include = true;
                break;
            }
        }
        if (!include) {
            return null;
        }

        if (temp.startsWith("&") || temp.startsWith("?")) {
            return null;
        }

        if (temp.contains("&")) {
            temp = temp.split("&")[0];
        }
        if (temp.contains("?")) {
            temp = temp.split("[?]")[0];
        }
        return temp;
    }


    public static String getHostFromURL(String s) {
        if (s == null || s.trim().length() == 0) {
            return null;
        }

        String temp = s.trim();
        String[] prefix = {"http://", "https://", "ftp://"};
        for (String p : prefix) {
            if (temp.startsWith(p)) {
                temp = temp.substring(p.length()).trim();
            }
        }

        String host = StringUtils.multipleSplit(temp, "/", "[?]", ":", "%");
        if (host == null) {
            return "Invalid URL";
        }

        if (host.matches(IP_REGEX)) {
            return host;
        }
        String[] field = host.split("[.]");
        int len = field.length;
        if (field.length < 2)
            return host;

        if ((country.contains(field[len - 1].toLowerCase())) && len > 2 && (!field[len - 3].equals("www"))) {
            return field[len - 3] + "." + field[len - 2] + "." + field[len - 1];
        } else {
            return field[len - 2] + "." + field[len - 1];
        }
    }


    public static void main(String[] argc) {


    }


}
