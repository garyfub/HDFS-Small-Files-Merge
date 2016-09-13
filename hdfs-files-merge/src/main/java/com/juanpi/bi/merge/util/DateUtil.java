package com.juanpi.bi.merge.util;

import org.apache.hadoop.fs.Path;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;


/**
 * Created by gongzi on 2016/9/9.
 */
public class DateUtil {

    /**
     *
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

    public static String getHourIntervalMillis(int hourInterval, String fmt) {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.HOUR, hourInterval);
        String hourIntervalDate = new SimpleDateFormat(fmt).format(cal.getTime());

        return hourIntervalDate;
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

    public static void main(String[] args){
        String dateHourStr = DateUtil.dateHourStr("1473351420000", "yyyyMMddHH");
        System.out.println(dateHourStr);
        System.out.println(millisToMins("1473351420000"));

        String fmt = "yyyy-MM-dd HH:00:00";
        String dt = DateUtil.getHourIntervalDate(0, fmt);
        try {
            long milis = DateUtil.dateToMillis(dt, fmt);
            System.out.println("milis==" + milis);
        } catch (ParseException e) {
            e.printStackTrace();
        }


        HashMap<String, List<Path>> filesMap = new HashMap<>();

        if(null == filesMap.get("a") || filesMap.get("a").isEmpty())
        {
            System.out.println("filesMap.isEmpty");
        }
    }


}
