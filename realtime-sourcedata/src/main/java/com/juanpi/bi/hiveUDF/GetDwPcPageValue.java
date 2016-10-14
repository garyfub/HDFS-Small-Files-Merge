package com.juanpi.bi.hiveUDF;

import com.juanpi.bi.utils.DecodeURLParam;
import com.juanpi.bi.utils.DwPageValue;
import com.juanpi.bi.utils.DwPageValue.Page_Pattern;
import org.apache.commons.lang.StringUtils;

import java.io.InputStream;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 获取pc端pagevalue
 * 2015年8月28日11:08:05
 */
public class GetDwPcPageValue // extends UDF
{
    public String page_value;
    public String page_type_id;
    private Pattern pattern = Pattern
            .compile("keywords=([\u4E00-\u9FA5\\s0-9A-Za-z%+-]+)");
    private Pattern pattern1 = Pattern
            .compile("keyword=([\u4E00-\u9FA5\\s0-9A-Za-z%+-]+)");
    private Pattern pattern3 = Pattern
            .compile("(deal|jump|shop|click|click/auth|appbrand)?/?(\\d+)");
    private Pattern pattern5 = Pattern.compile("(zhuanti|act|event)/([a-zA-Z0-9_]+)");
    // by lielie,
    // by gongzi 拼团回流ShareReflux， (singlemessage|groupmessage|timeline) 这3个分享回流关键词直接在page_ref_reg和event_reg的sql中处理
    // by gongzi 拼团和抽奖团
    private Pattern pattern11 = Pattern.compile("/(shop|item)?/?(\\d+)");
    private Pattern pattern12 = Pattern.compile("landing/(app_[0-9]+)");

    public String evaluate(String url) {

        if (url == null || url.trim().length() == 0) {
            System.out.println("url is null");
            return null;
        }

        DwPageValue.getInstance("PcPageValue.properties");
        Map<String, Page_Pattern> patternsMap = DwPageValue.patternsMap;

        if (patternsMap == null || patternsMap.isEmpty()) {
            return null;
        }

        // 获取page_type_id
        page_value = getPageValue(url);

        if (!patternsMap.containsKey(page_value)) {
            return null;
        }

        Page_Pattern pat = patternsMap.get(page_value);

        //通过page_type_id定位
        //搜索页获取关键词规则
        if ("6".equals(pat.page_type_id)) {
            try {
                String result = null;
                String res = null;
                if (url == null) {
                    return result;
                }
                DecodeURLParam d = new DecodeURLParam();
                String urlString = url.trim().replace("_", "--").toLowerCase();
                Matcher matcher = pattern.matcher(urlString);
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
            //商祥页/品牌页/Jump页解析规则
        } else if ("3".equals(pat.page_type_id) || "7".equals(pat.page_type_id)
                || "8".equals(pat.page_type_id) || "9".equals(pat.page_type_id)) {

            String result = "";
            if (url == null)
                url = "";
            url = url.toLowerCase();
            Matcher matcher = pattern3.matcher(url);
            if (matcher.find()) {
                result = matcher.group(2);
            }
            result = decodeGoodid(result);
            return result;
            //活动页解析规则
        } else if (pat.page_type_id.equals("5")) {
            if (url == null || url.trim().length() == 0) {
                return null;
            }
            Matcher m = pattern5.matcher(url);
            if (m.find()) {
                return m.group().toLowerCase();
            }
            return null;
            // 拼团页，匹配解析
        } else if (pat.page_type_id.equals("11")) {
            if (url == null || url.trim().length() == 0) {
                return null;
            }
            Matcher m = pattern11.matcher(url);
            if (m.find()) {
                String res;
                if (url.contains("shop") || url.contains("item")) {
                    res = m.group(2);
                    res = decodeGoodid(res);

                } else {
                    res = m.group().toLowerCase();
                }
                return res;
            }
            return null;
            //类目页，在文件里解析
        } else if (pat.page_type_id.equals("12")) {
            if (url == null || url.trim().length() == 0) {
                return null;
            }
            Matcher m = pattern12.matcher(url);
            if (m.find()) {
                return m.group().toLowerCase();
            }
            return null;
            //类目页，在文件里解析
        } else if ("4".equals(pat.page_type_id)) {
            return pat.page_value;
        } else {
            return null;
        }

    }

    private static StringBuilder sb = new StringBuilder();

    /**
     * @param url
     * @return
     */
    public static String getPageValue(String url) {
        if (url == null || url.trim().length() < 4) {
            return null;
        }
        url = url.trim();
        List<Page_Pattern> patterns = DwPageValue.patterns;
        for (Page_Pattern pat : patterns) {
            if (pat.match(url)) {
                return pat.page_value;
            }
        }

        return "0";
    }

    /**
     * @param goodid
     * @return
     */
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
        GetDwPcPageValue gh = new GetDwPcPageValue();
        {
            List<String> urls = Arrays.asList(
                "http://shop.juanpi.com/deal/26452941"
                , "http://tuan.juanpi.com/pintuan/shop/1881480"
                , "http://tuan.juanpi.com/pintuan/item/1881480"
                , "http://tuan.juanpi.com/pintuan/shop/1756661"
                , "http://shop.juanpi.com/deal/21683610"
//					,"http://tuan.juanpi.com/pintuanlottery/shop/1384600"
//					,"http://tuan.juanpi.com/pintuan/shop/1881480?itemid=194560"
//					,"http://wx.juanpi.com/pintuan/shop/1881480"
                , "http://wx.juanpi.com/pintuan/shop/1881480?itemid=194560"
                , "http://tuan.juanpi.com/pintuan/shop/1574696"
                , "http://wx.juanpi.com/pintuan/shop/1726634?utm=105872&from=singlemessage&isappinstalled=0"
                , "http://tuan.juanpi.com/pintuan/item/10029910?utm=106501&from=groupmessage&isappinstalled=0"
//                    ,"http://wx.juanpi.com/pintuan/item/2054187?utm=106747&from=singlemessage&isappinstalled=0"
//					,"http://wx.juanpi.com/brand/1708451?shop_id=1200642&from=singlemessage&isappinstalled=1"
//					"http://m.juanpi.com/brand/appbrand/1751592?shop_id=1949563",
//					"http://m.juanpi.com/act/skcbrand",
//					"http://m.juanpi.com/landing/app_1445309805"
            );

            String pageValue;
            for (String url : urls) {
                pageValue = gh.evaluate(url);
                System.out.println("pageValue = " + pageValue + ", " + url);
            }
        }
    }
}
