package com.juanpi.bi.merge.util;

import java.util.List;
/**
 * 
 * @author yunduan  
 * @date 2016年6月23日 上午10:58:52    
 *
 */
public class ProcessUtil {

	/**
	 * 判断集合是否为空
	 * 
	 * @param list
	 *            输入集合
	 * @return
	 */
	public static boolean isNull(List<?> list) {
		if (list == null) {
			return true;
		}
		if (list.size() == 0) {
			return true;
		}

		return false;
	}

	/**
	 * 判断字符串是否为空
	 * 
	 * @param input
	 *            字符串
	 * @return
	 */
	public static boolean isNull(String input) {
		if (input == null) {
			return true;
		}

		if (" ".equals(input)) {
			return true;
		}

		input = input.trim();
		if (input.length() == 0) {
			return true;
		}

		if (input.toLowerCase().equals("null")) {
			return true;
		}

		return false;
	}

	public static String stackTraceMsg(Exception e) {
		String traceMessage = e.getMessage();
		StackTraceElement[] trace = e.getStackTrace();
		for (StackTraceElement traceElement : trace) {
			traceMessage += "\n\t" + traceElement;
		}

		return traceMessage;
	}
	
}
