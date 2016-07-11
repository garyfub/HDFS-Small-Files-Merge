package com.juanpi.bi.utils;

import java.util.List;

/**
 * @author zhuokun
 */
public class PageTypeUtils {

    public static enum DataType {
        LONG, DOUBLE, INTEGER, STRING
    }

    public static final String[] SITE_HOSTS = {"juanpi.com", "jiukuaiyou.com"};

    public static Integer getPageID(String url) {
        if (url == null || url.trim().length() < 4) {
            return null;
        }
        url = url.trim();
        if (-999 == GetSiteId.evaluate(url)) {
            // 站外页，直接返回
            return -999;
        }

        List<PageIDChecker.Page_Pattern> patterns = PageIDChecker.patterns;
        for (PageIDChecker.Page_Pattern pat : patterns) {
            if (pat.match(url)) {
                return pat.id;
            }
        }
        return -1;

    }

    public static Integer getSiteID(String url) {
        if (url == null || url.trim().length() < 4) {
            return null;
        }
        url = url.toLowerCase().trim();
        if (URLUtils.getHostFromURL(url) != null) {
            if ("yihaodian.com".equals(URLUtils.getHostFromURL(url))
                    || "yhd.com".equals(URLUtils.getHostFromURL(url))) {
                return 1;
            } else if ("1mall.com".equals(URLUtils.getHostFromURL(url))) {
                return 2;
            } else if ("111.com.cn".equals(URLUtils.getHostFromURL(url))) {
                return 3;
            } else {
                return 0;
            }
        }
        return null;
    }

}
