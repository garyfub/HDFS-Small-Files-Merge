package com.juanpi.bi.commonUtils;



import org.apache.commons.lang.StringUtils;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Description: TODO <br/>
 * <p/>
 * <b>修改历史:</b> <br/>
 * 2014年9月27日 xiaopang add <br/>
 */
public class SearchEnginKeywordUtil {
	
	private static String encode = StringConstants.utf8;
	private static final Map<String, Pattern> patternMap = new HashMap<String, Pattern>();


	static {
		patternMap.put("google", Pattern.compile("[\\&|\\?]q=([^\\&]*)|[\\&|\\?]oq=([^\\&]*)"));
		patternMap.put("iask", Pattern.compile("[\\&|\\?]k=([^\\&]*)|[\\&|\\?]_searchkey=([^\\&]*)"));
		patternMap.put("sogou", Pattern.compile("[\\&|\\?]query=([^\\&]*)"));
		patternMap.put("163", Pattern.compile("[\\&|\\?]q=([^\\&]*)"));
		patternMap.put("yahoo", Pattern.compile("[\\&|\\?]p=([^\\&]*)|[\\&|\\?]q=([^\\&]*)"));
		patternMap.put("baidu", Pattern.compile("[\\&|\\?]wd=([^\\&]*)|[\\&|\\?]word=([^\\&]*)"));
		patternMap.put("lycos", Pattern.compile("[\\&|\\?]query=([^\\&]*)"));
		patternMap.put("aol", Pattern.compile("[\\&|\\?]encquery=([^\\&]*)"));
		patternMap.put("3721", Pattern.compile("[\\&|\\?]p=([^\\&]*)|[\\&|\\?]name=([^\\&]*)"));
		patternMap.put("search", Pattern.compile("[\\&|\\?]q=([^\\&]*)"));
		patternMap.put("soso", Pattern.compile("[\\&|\\?]w=([^\\&]*)"));
		patternMap.put("zhongsou", Pattern.compile("[\\&|\\?]w=([^\\&]*)"));
		patternMap.put("alexa", Pattern.compile("[\\&|\\?]q=([^\\&]*)"));
		patternMap.put("bing", Pattern.compile("[\\&|\\?]q=([^\\&]*)"));
		patternMap.put("yisou", Pattern.compile("[\\&|\\?]q=([^\\&]*)"));
		patternMap.put("sina", Pattern.compile("[\\&|\\?]q=([^\\&]*)"));
		patternMap.put("so", Pattern.compile("[\\&|\\?]q=([^\\&]*)"));
		patternMap.put("360", Pattern.compile("[\\&|\\?]q=([^\\&]*)"));
		patternMap.put("youdao", Pattern.compile("[\\&|\\?]q=([^\\&]*)"));
	}
	
	public static String getKeyword(String url, String searchEngine) {
		if (StringUtils.isBlank(url) == true) {
			return StringUtils.EMPTY;
		}
		if ((checkCode("3721|iask|163|zhongsou",searchEngine)
				|| (checkCode("baidu|soso", searchEngine)&&!checkCode("m.baidu.com", url.toLowerCase()) && !checkCode("ie=utf-8", url.toLowerCase()))
				|| (checkCode("sogou", searchEngine) && !checkCode("ie=utf8", url.toLowerCase()))
				)) {
			setEncode("GBK");
		}
		
		return null;
	}


	@SuppressWarnings("unused")
    private static String decoderKeyword(final Matcher m, final String refererUrl) {
		String keyword = "";
		try
		{
			String encode = "UTF-8";
			String searchEngine = getSearchEngine(refererUrl);
			if (StringUtils.isBlank(searchEngine) == false) {
				if ((checkCode("3721|iask|163|zhongsou",searchEngine) 
						|| (checkCode("baidu|soso", searchEngine)&&!checkCode("m.baidu.com", refererUrl.toLowerCase()) && !checkCode("ie=utf-8", refererUrl.toLowerCase()))
						|| (checkCode("sogou", searchEngine) && !checkCode("ie=utf8", refererUrl.toLowerCase()))
						)) {
					encode = "GBK";
				}
	
				if (m.find()) {
					for (int i = 2; i <= m.groupCount(); i++) {
						if (m.group(i) != null)// 在这里对关键字分组就用到了
						{
							try {
								keyword = URLDecoder.decode(m.group(i), encode);
							} catch (UnsupportedEncodingException e) {
								System.out.println(e.getMessage());
							}
							break;
						}
					}
				}
			}
		}catch (Exception e) {
		}
		return keyword;
	}

	public static String  getSearchEngine(String refUrl) {
		if (refUrl.length() > 11) {
			// p是匹配各种搜索引擎的正则表达式
			Pattern p = Pattern
					.compile("[http|https]:\\/\\/.*\\.(google\\.com(:\\d{1,}){0,1}| "
							+ "google\\.cn(:\\d{1,}){0,1}|baidu\\.com(:\\d{1,}){0,1}|"
							+ "yahoo\\.com(:\\d{1,}){0,1}|iask\\.com(:\\d{1,}){0,1}|"
							+ "yahoo\\.cn(:\\d{1,}){0,1}|youdao\\.com(:\\d{1,}){0,1}|"
							+ "sogou\\.com(:\\d{1,}){0,1}|163\\.com(:\\d{1,}){0,1}|"
							+ "lycos\\.com(:\\d{1,}){0,1}|aol\\.com(:\\d{1,}){0,1}|"
							+ "3721\\.com(:\\d{1,}){0,1}|search\\.com(:\\d{1,}){0,1}|"
							+ "soso\\.com(:\\d{1,}){0,1}|zhongsou\\.com(:\\d{1,}){0,1}|"
							+ "so\\.com(:\\d{1,}){0,1}|bing\\.com(:\\d{1,}){0,1}|"
							+ "360\\.cn(:\\d{1,}){0,1}|2345\\.com(:\\d{1,}){0,1}|"
                            + "hao123\\.com(:\\d{1,}){0,1}|"
							+ "yisou\\.com(:\\d{1,}){0,1}|alexa\\.com(:\\d{1,}){0,1})");
			Matcher m = p.matcher(refUrl);
			if (m.find()) {
				return insteadCode(m.group(1), "()", "");
			}
		}
		return null;
	}

	private static String insteadCode(String str, String regEx, String code) {
		Pattern p = Pattern.compile(regEx);
		Matcher m = p.matcher(str);
		String s = m.replaceAll(code);
		return s;
	}

	private static boolean checkCode(String regEx, String str) {
		Pattern p = Pattern.compile(regEx);
		Matcher m = p.matcher(str);
		return m.find();
	}
    public static String[] getSearchEngineAndKeyword(String referer){
        String searchEngine = getSearchEngine(referer);
        if(StringUtils.isEmpty(searchEngine))
            return null;
        String keyword = getKeyword(referer, searchEngine);
        
        String[] strArr = {searchEngine, keyword};
        return strArr;
    }


    public static String getEncode() {
        return encode;
    }


    public static void setEncode(String encode) {
        SearchEnginKeywordUtil.encode = encode;
    }

}