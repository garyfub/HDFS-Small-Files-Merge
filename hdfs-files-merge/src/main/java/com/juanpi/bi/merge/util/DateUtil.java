package com.juanpi.bi.merge.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;


/**
 * Created by gongzi on 2016/9/9.
 */
public class DateUtil {

    static String AM0 = "00";
    static String AM0_FMT = "yyyy-MM-dd 23:59:59";

    /**
     * 传入Hour时间间隔，返回指定的呵合乎规则的日期
     * @param hourInterval 间隔的小时数量
     * @param fmt
     * @return
     */
    public static String getHourIntervalDate(int hourInterval, String fmt) {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.HOUR, hourInterval);
        String hourIntervalDate = new SimpleDateFormat(fmt).format(cal.getTime());

        return hourIntervalDate;
    }

    /**
     * 一小时前日期; 格式:yyyy-MM-dd HH
     * @return
     */
    public static String getOneHourAgoDate() {
        String oneHourAgoDate = getHourIntervalDate(-1, "yyyy-MM-dd");
        return oneHourAgoDate;
    }

    /**
     * 传入日期时间间隔，返回指定的呵合乎规则的日期
     * @return
     */
    public static String getDateIntervalDate(int dateInterval, String fmt) {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, dateInterval);
        String dateIntervalDate = new SimpleDateFormat(fmt).format(cal.getTime());

        return dateIntervalDate;
    }

    /**
     *
     * @return
     */
    public static String getOneDateAgoDate() {
        String beforeOneHourDate = getDateIntervalDate(-1, "yyyy-MM-dd");
        return beforeOneHourDate;
    }

    /**
     * 系统当前时间间隔的x小时的时间戳
     * @param hourInterval
     * @return
     */
    public static long getHourIntervalMillis(int hourInterval) {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.HOUR, hourInterval);
        return cal.getTimeInMillis();
    }

    /**
     *
     * @param milliSeconds String
     * @param fmt yyyy-MM-dd, yyyyMMddHH etc
     * @return
     */
    public static String dateHourStr(String milliSeconds, String fmt){
        Date dt = milliSecondsToDate(strToLong(milliSeconds));
        String beforeOneHourDate = new SimpleDateFormat(fmt).format(dt);
        return beforeOneHourDate;
    }

    /**
     * milliseconds string to Date
     * @param timeSTamp the milliseconds since January 1, 1970, 00:00:00 GMT.
     * @return
     */
    public static Date milliSecondsToDate(Long timeSTamp)
    {
         return new Date(timeSTamp);
    }

    /**
     *
     * @param dt
     * @param fmt 格式 yyyy-MM-dd HH etc.
     * @return
     * @throws ParseException
     */
    public static long dateToMillis(String dt, String fmt) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(fmt);
        Date date = sdf.parse(dt);
        return date.getTime();
    }

    /**
     * String to Long
     * @param numStr 数值型字符串
     * @return
     */
    public static Long strToLong(String numStr)
    {
        return Long.parseLong(numStr);
    }

//    Converting Milliseconds to Minutes and Seconds
    public static int millisToMins (String milliSeconds)
    {
//        int seconds = (int) ((milliseconds / 1000) % 60);
        int minutes = (int) ((strToLong(milliSeconds) / 1000) / 60);
        return minutes;
    }

    /**
     * 返回一个小时前的日期的毫秒值
     * 如果是零点的日期，依然处理上一个小时的数据
     * @return
     */
    public static long getHoursAgoMillis()
    {
        Calendar cal = Calendar.getInstance();
        long milis = cal.getTimeInMillis();
        String fmt = "yyyy-MM-dd HH:00:00";
        String dt = DateUtil.getHourIntervalDate(0, fmt);
        String hourStr = dt.substring(11, 11+2);

        if(AM0.equals(hourStr))
        {
            // 当前天减一
            dt = getDateIntervalDate(-1, AM0_FMT);
            fmt = "yyyy-MM-dd HH:mm:ss";
        }

        try {
            milis = DateUtil.dateToMillis(dt, fmt);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return milis;
    }

    private static long dddddd()
    {
        Calendar cal = Calendar.getInstance();
//        Month value is 0-based. e.g., 0 for January.
        cal.set(2016, 9-1, 23, 01, 10, 0); //2016-09-23 00:01:00
        long milis = cal.getTimeInMillis();

        String fmt = "yyyy-MM-dd HH:00:00";
        String dt = new SimpleDateFormat(fmt).format(cal.getTime());
        System.out.println("=====" + dt);
        String hourStr = dt.substring(11, 11+2);
        if("00".equals(hourStr))
        {
            // 当前天减一
            dt = getDateIntervalDate(-1, AM0_FMT);
            fmt = "yyyy-MM-dd HH:mm:ss";
        }

        try {
            milis = DateUtil.dateToMillis(dt, fmt);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return milis;
    }

    /**
     * 指定日期和日期间隔，返回间隔之前的日期
     * @param specifiedDay
     * @param interval
     * @return
     */
    public static String getSpecifiedDayAgo(String specifiedDay, int interval){
        return getSpecifiedDay(specifiedDay, interval, "-");
    }

    /**
     * 指定日期和日期间隔，返回间隔之前的日期
     * @param specifiedDay
     * @param interval
     * @return
     */
    public static String getSpecifiedDayAfter(String specifiedDay, int interval){
        return getSpecifiedDay(specifiedDay, interval, "+");
    }

    private static String getSpecifiedDay(String specifiedDay, int interval, String func) {
        Calendar c = Calendar.getInstance();
        Date date = null;
        try {
            date = new SimpleDateFormat("yyyy-MM-dd").parse(specifiedDay);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        c.setTime(date);
        int day=c.get(Calendar.DATE);

        if("+".equals(func)) {
            c.set(Calendar.DATE, day + interval);
        } else if("-".equals(func)) {
            c.set(Calendar.DATE, day - interval);
        } else {
            return null;
        }

        String dayAfter = new SimpleDateFormat("yyyy-MM-dd").format(c.getTime());
        return dayAfter;
    }

    public static void main(String[] args){
        String d1 = getSpecifiedDayAfter("2017-01-11", 7);
        String d2 = getSpecifiedDayAgo("2017-01-11", 7);
        System.out.println(d1);
        System.out.println(d2);
    }
}
