package com.juanpi.bi.merge.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;
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

	public static String getYesterday() {
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DATE, -1);
		String yesterday = new SimpleDateFormat("yyyy-MM-dd ").format(cal.getTime());
		
		return yesterday;
	}
	
	/**
	 * 一小时前日期; 格式:yyyy-MM-dd HH
	 * @return
	 */
	public static String getBeforeOneHourDate() {
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.HOUR, -1);
		String beforeOneHourDate = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime());
		
		return beforeOneHourDate;
	}
	
	/**
	 * 获取上一小时时间
	 * @return
	 */
	public static String getBeforeOneHour() {
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.HOUR, -1);
		String beforeOneHour = new SimpleDateFormat("HH").format(cal.getTime());
		
		return beforeOneHour;
	}
	
}
