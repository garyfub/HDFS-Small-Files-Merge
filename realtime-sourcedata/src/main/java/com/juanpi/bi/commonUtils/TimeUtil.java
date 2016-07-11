package com.juanpi.bi.commonUtils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;

/**
 * 
 * Description: 日期时间工具类 <br/>
 * 
 * <b>修改历史:</b> <br/>
 * Sep 4, 2014 baicai 增加注释 <br/>
 */
public final class TimeUtil {

    public static DateFormat format4YMDHM = new SimpleDateFormat("yyyy-MM-dd HH:mm");
    public static DateFormat format4YMD = new SimpleDateFormat("yyyy-MM-dd");
    public static final DateFormat datetimeFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z",Locale.US);
    
    /**
     * 
     * UTC2Date:表示日期的字符串转日期 <br/>
     * 
     * @param dateString
     * @return Date
     * @throws java.text.ParseException
     *            日期字符串格式错误 <br/>
     */
    public static Date UTC2Date(String dateString) throws ParseException {
        //dateString 样本数据  31/Aug/2014:21:26:28 +0800
        return datetimeFormat.parse(dateString);
            
    }
    
    /**
     * 
     * UTC2Timestamp:表示日期的字符串转时间戳 <br/>
     * 
     * @param dateString
     * @return long
     * @throws java.text.ParseException
     *            日期字符串格式错误 <br/>
     */
    public static long UTC2Timestamp(String dateString) throws ParseException {
        //dateString 样本数据  31/Aug/2014:21:26:28 +0800
        return (long) (datetimeFormat.parse(dateString).getTime() );
            
    }
    
    /**
     * 
     * UTC2Timestamp:时间 <br/>
     * 
     * @param time
     * @return long
     * @throws java.text.ParseException
     *            日期字符串格式错误 <br/>
     */
    public static Date Timestamp2Date(long time) throws ParseException {
        Date date = new Date(time);
        return date;       
    }

    public static String Timestamp2Str(long time)  {
        Date date = new Date(time);
        return format4YMDHM.format(date);      
    }

    /**
     * 获得指定日期的前几天
     *
     * @param dateStr
     * @param n
     * @return
     * @throws Exception
     */
    public static Date getDayBefore(String dateStr,int n) {//可以用new Date().toLocalString()传递参数
        
        Calendar c = Calendar.getInstance();
        try {
            c.setTime(TimeUtil.dateStr2Date(dateStr));
        } catch (ParseException e) {

            e.printStackTrace();
        }
        int day = c.get(Calendar.DATE);
        c.set(Calendar.DATE, day - n);

        return c.getTime();
    }

    /**
     * 获得指定日期的后几天
     *
     * @param date
     * @param n
     * @return
     */
    public static Date getDayAfter(Date date,int n) {

        Calendar c = Calendar.getInstance();
        c.setTime(date);
        int day = c.get(Calendar.DATE);
        c.set(Calendar.DATE, day + n);

        return c.getTime();
    }

    /**
     * 计算两日期的天数差
     * @param date1
     * @param date2
     * @return
     */
    public static int diffdates(Date date1, Date date2) {
        int result = 0;
        GregorianCalendar gc1 = new GregorianCalendar();
        GregorianCalendar gc2 = new GregorianCalendar();
        gc1.setTime(date1);
        gc2.setTime(date2);
        result = diffdates(gc1, gc2);
        return result;
    }

    /**
     *
     * @param g1
     * @param g2
     * @return
     */
    public static int diffdates(GregorianCalendar g1, GregorianCalendar g2) {
        int elapsed = 0;
        GregorianCalendar gc1, gc2; if (g2.after(g1)) {
            gc2 = (GregorianCalendar) g2.clone();
            gc1 = (GregorianCalendar) g1.clone();} else {gc2 = (GregorianCalendar) g1.clone();
            gc1 = (GregorianCalendar) g2.clone();
        } gc1.clear(Calendar.MILLISECOND);
        gc1.clear(Calendar.SECOND);
        gc1.clear(Calendar.MINUTE);
        gc1.clear(Calendar.HOUR_OF_DAY); gc2.clear(Calendar.MILLISECOND);
        gc2.clear(Calendar.SECOND);
        gc2.clear(Calendar.MINUTE);
        gc2.clear(Calendar.HOUR_OF_DAY);
        while (gc1.before(gc2)) {
            gc1.add(Calendar.DATE, 1);elapsed++;
        }
        return elapsed;
    }

    public static Date dateStr2Date(String dateString) throws ParseException {
        return format4YMD.parse(dateString);
    }


    /**
     * date根据自定义格式转字符串
     * @param date
     * @param format
     * @return
     * @throws ParseException
     */
    public static String date2String(Date date, DateFormat format) throws ParseException {
        return format.format(date);
    }
}




