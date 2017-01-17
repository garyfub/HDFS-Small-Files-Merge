/**
 *
 * @(#)DateUtil.java TODO版本信息  2014年10月10日
 * Copyright © 2010 - 2014 JuanPi.com 
 * All Rights Reserved   
 *
 **/
package com.juanpi.bi.commonUtils;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

/**
 * Description: TODO <br/>
 * 
 * <b>修改历史:</b> <br/>
 * 2014年10月10日 xiaopang Update Description <br/>
 */
public class DateUtil {

    public static Calendar cald = GregorianCalendar.getInstance();

    public static String mDateStringFormat = "yyyy-MM-dd";
    public static String mTimeStringFormat = "HH:mm:ss";
    public static String mDateTimeStringFormat = "yyyy-MM-dd HH:mm:ss";
    public final static long ONE_DAY_MILLISECONDS = 86400000;
    
    /**
     * 以当前时间，建立一个DateUtil
     */
    public DateUtil() {
        cald = GregorianCalendar.getInstance();
    }

    /**
     * 以给定的long型的时间戳，建立一个DateUtil，这个时间戳是和本地time zone 无关的
     * 
     * @param time
     */
    public DateUtil(long time) {
        cald = GregorianCalendar.getInstance();
        setTime(time);
    }

    /**
     * 用字符串时间来建立一个DateUtil
     * 
     * @param dt
     * @throws java.text.ParseException
     */
    public DateUtil(String dt) throws ParseException {
        cald = GregorianCalendar.getInstance();
        setTime(dt);
    }

    /**
     * 用给定的时间，建立一个dateUtil类
     * 
     * @param year
     * @param month
     * @param day
     * @param hour
     * @param minute
     * @param second
     */
    public DateUtil(int year, int month, int day, int hour, int minute, int second) {
        cald = GregorianCalendar.getInstance();
        setTime(year, month, day, hour, minute, second);
    }

    /**
     * 用一个时间戳来设定当前的DateUtil，，这个时间戳是和本地time zone 无关的
     * 
     * @param time
     */
    public static void setTime(long time) {
        Date datetime = new Date(time);
        cald.setTime(datetime);
    }

    /**
     * 用一个字符串时间来设定DateUtil的时间，该字符串必须符合一定的格式，缺省的 格式是yyyy-MM-dd hh:mm:ss，
     * 如果格式不一样，可以通过调用 setDateTimeStringFormat 设定
     * 
     * @param dt
     * @throws java.text.ParseException
     */
    public static void setTime(String dt) throws ParseException {
        SimpleDateFormat mDateFormat = new SimpleDateFormat(mDateTimeStringFormat);
        Date datetime = mDateFormat.parse(dt);
        cald.setTime(datetime);
    }

    /**
     * 设置年、月、日、时、分、秒，注意这些是local time zone 时间
     * 
     * @param year
     * @param month
     * @param day
     * @param hour
     * @param minute
     * @param second
     */
    public static void setTime(int year, int month, int day, int hour, int minute, int second) {
        cald.set(year, month, day, hour, minute, second);
    }

    /**
     * 设置年、月、日
     * 
     * @param year
     * @param month
     * @param day
     */
    public static void setDateTime(int year, int month, int day) {
        cald.set(year, month, day);
    }

    /**
     * 设置时、分、秒
     * 
     * @param hour
     * @param minute
     * @param second
     */
    public static void setTimeTime(int hour, int minute, int second) {
        cald.set(Calendar.HOUR_OF_DAY, hour);
        cald.set(Calendar.MINUTE, minute);
        cald.set(Calendar.SECOND, second);
    }

    /**
     * 设置时、分
     * 
     * @param hour
     * @param minute
     *
     */
    public static void setShortTimeTime(int hour, int minute) {
        cald.set(Calendar.HOUR, hour);
        cald.set(Calendar.MINUTE, minute);
    }

    public static String getDateString() {
        SimpleDateFormat mDateFormat = new SimpleDateFormat(mDateStringFormat);
        return mDateFormat.format(cald.getTime());
    }

    public static String getTimeString() {
        SimpleDateFormat mDateFormat = new SimpleDateFormat(mTimeStringFormat);
        return mDateFormat.format(cald.getTime());
    }

    //根据指定日期，返回时间戳
    public static long getTimestamp(String dateStr, String dateFormat) {
        SimpleDateFormat mDateFormat = new SimpleDateFormat(dateFormat);
        Date date = null;
        try {
            date = mDateFormat.parse(dateStr);
        } catch (ParseException e) {
            e.printStackTrace();
        } 
        return date.getTime();
    }
    
    //返回当前时间戳
    public static long getCurTimestamp(){
        return new Timestamp(cald.getTimeInMillis()).getTime();
    }
    
    //返回当日0点时间戳，
    public static long getTodayZeroTimestamp(){

        cald.set(Calendar.HOUR_OF_DAY, 0); 
        cald.set(Calendar.SECOND, 0); 
        cald.set(Calendar.MINUTE, 0); 
        cald.set(Calendar.MILLISECOND, 0); 

        return new Timestamp(cald.getTimeInMillis()).getTime();
    }
    
    
    /**
     * 返回unix时间戳，这个是和time zone 没有关系的
     * 
     * @return
     */
    public static long getMillsecond() {
        return cald.getTime().getTime();
    }

    public static int getYear() {
        return cald.get(Calendar.YEAR);
    }

    public static int getMonth() {
        return cald.get(Calendar.MONTH);
    }

    public static int getDay() {
        return cald.get(Calendar.DAY_OF_MONTH);
    }

    public static int getWeek() {
        return cald.get(Calendar.DAY_OF_WEEK);
    }

    public static int getHour() {
        return cald.get(Calendar.HOUR_OF_DAY);
    }

    public static int getMinute() {
        return cald.get(Calendar.MINUTE);
    }

    public static int getSecond() {
        return cald.get(Calendar.SECOND);
    }

    public static void setDateStringFormat(String dsf) {
        mDateStringFormat = dsf;
    }

    public static void setTimeStringFormat(String tsf) {
        mTimeStringFormat = tsf;
    }

    public static void setDateTimeStringFormat(String tsf) {
        mDateTimeStringFormat = tsf;
    }

    public static long getDayStartTick() {
        setTimeTime(0, 0, 0);
        return ((long) (getMillsecond() / 1000.0)) * 1000;// 去掉小于一秒造成的不同
    }

    public static long getDayEndTick() {
        return getDayStartTick() + ONE_DAY_MILLISECONDS;
    }

    public static float getLiveDay(long birthday) {
        long curr = System.currentTimeMillis();
        float days = (curr - birthday) / (24 * 60 * 60 * 1000);
        return days;
    }
}
