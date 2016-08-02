package com.juanpi.bi.hiveUDF;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 获取mbpagevalue
 * 用法：GetMbPageValue(extend_params,page_type_id)
 */
public class GetDwMbPageValue // extends UDF
{
	private Pattern p = Pattern.compile("(zhuanti|act|event)/([a-zA-Z0-9]+)");

	public String evaluate(String extend_params, String page_type_id) {
		if (page_type_id == null || page_type_id.trim().length() == 0) {
			return null;
		}
		if (extend_params == null || extend_params.trim().length() == 0) {
			return null;
		}
		extend_params = extend_params.trim().toLowerCase();

		if ("5".equals(page_type_id)) {
			if (extend_params == null || extend_params.trim().length() == 0) {
				return null;
			}
			Matcher m = p.matcher(extend_params);
			if (m.find()) {
				return m.group().toLowerCase();
			}
			return null;
		}  else {
			return extend_params.toLowerCase();
		}
	}

	public static void main(String[] argc) {
		GetDwMbPageValue gh = new GetDwMbPageValue();
		String extend_params = "http://www.juanpi.com/zhuanti/kxyx";
		String page_type_id ="5";
		System.out.println(gh.evaluate(extend_params, page_type_id));
	}
}