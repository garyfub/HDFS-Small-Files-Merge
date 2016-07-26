package com.juanpi.bi.commonUtils;

import nl.bitwalker.useragentutils.OperatingSystem;
import nl.bitwalker.useragentutils.UserAgent;
import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

/**
 * Description: 日志解析方法类 <br/>
 * <p/>
 * <b>修改历史:</b> <br/>
 * Sep 1, 2014 xiaopang init <br/>
 * Sep 4, 2014 baicai 增加注释 <br/>
 */
public class ParseUtil {

    private static final String SPIDER = "spider";
    private static final String BOT = "bot";
    private static final String ISSPIDER = "isSpider";
    private static final String RENDERRINGENGINE = "renderingEngine";
    private static final String BROWSER = "browser";
    private static final String BROWSERTYPE = "browserType";
    private static final String ISMOBILEDEVICE = "isMobileDevice";
    private static final String OPERATINGSYSTEM = "operatingSystem";


    private static StringBuilder sb = new StringBuilder();
    public static Map<String, String> userAgentMap = new HashMap<String, String>();

    /**
     * decodeUidUtmid:uid和utmid解码 <br/>
     *
     * @param value
     * @return String
     * TODO Description <br/>
     */
    public static String decodeUidUtmid(String value) {

        if (value.indexOf(";") >= 0) {
            value = value.substring(0, value.indexOf(";"));
        }

        if (value.trim().equals(StringUtils.EMPTY)) {
            return value;
        }

        try {
            value = String.valueOf(Long.parseLong(value, 36) - 60512868);
        } catch (Exception e) {
            value = StringUtils.EMPTY;
        }

        return value;
    }

    /**
     * decodeGoodid:goodid解码 <br/>
     *
     * @param goodid
     * @return String
     * TODO Description <br/>
     */
    public static String decodeGoodid(String goodid) {

        try{
            if (goodid.trim().equals(StringUtils.EMPTY)) {
                return goodid;
            }
            int l = goodid.length();
            Map<Integer, String> tmpstr = new HashMap<Integer, String>();
            int flag = 1;
            int c = 0;
            for(int i=0;i<l;i++) {
                if(i != 0 && i % 2 == 0) {
                    flag = -flag;
                    if(flag == 1) {
                        c++;
                    }
                }
                if(i == l -1) {
                    for(int j=0;j<l;j++) {
                        if(tmpstr.get(j) == null) {
                            tmpstr.put(j, String.valueOf(goodid.charAt(i)));
                        }
                    }
                } else {
                    if(i % 2 == 0) {
                        if(flag == 1) {
                            tmpstr.put(((int) i/2) - c, String.valueOf(goodid.charAt(i)));
                        } else {
                            tmpstr.put(((int) l/2) + c, String.valueOf(goodid.charAt(i)));
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
            for(Entry<Integer, String> e : treemap.entrySet()) {
                sb.append(e.getValue());
            }
            goodid = String.valueOf(Long.valueOf(sb.toString()) / 7 - 201341);
        }catch(Exception e) {
            goodid = StringUtils.EMPTY;
        }
        return goodid;
    }

    /**
     * parseUserAgent:解析userAgent信息 <br/>
     *
     * @param userAgentStr
     */
    public static void parseUserAgent(String userAgentStr) {
        String browser = StringUtils.EMPTY;
        String renderingEngine = StringUtils.EMPTY;
        String browserType = StringUtils.EMPTY;
        String operatingSystem = StringUtils.EMPTY;
        String isSpider = StringConstants.falseStr;
        String isMobileDevice = StringConstants.falseStr;

        //判断是否是爬虫
        String lowerCaseStr = userAgentStr.toLowerCase();
        if (lowerCaseStr.contains(SPIDER) ||
                lowerCaseStr.contains(BOT))
            isSpider = StringConstants.trueStr;
        userAgentMap.put(ISSPIDER, isSpider);

        //浏览器信息
        UserAgent userAgent = UserAgent.parseUserAgentString(userAgentStr);
        renderingEngine = userAgent.getBrowser().getRenderingEngine().name();
        browser = userAgent.getBrowser().getName();
        browserType = userAgent.getBrowser().getBrowserType().getName();
        userAgentMap.put(RENDERRINGENGINE, renderingEngine);
        userAgentMap.put(BROWSER, browser);
        userAgentMap.put(BROWSERTYPE, browserType);

        //操作系统信息
        OperatingSystem os = userAgent.getOperatingSystem();
        operatingSystem = os.getName();
        
        isMobileDevice = String.valueOf(os.isMobileDevice());
        userAgentMap.put(OPERATINGSYSTEM, operatingSystem);
        userAgentMap.put(ISMOBILEDEVICE, isMobileDevice);
    }

    /**
     * parseCookie:解析cookie <br/>
     *
     * @param cookieStr
     */
    public static void parseCookie(String cookieStr) {

    }


}
