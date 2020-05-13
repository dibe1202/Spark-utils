package com.longi.bigdata.javautils;

import org.apache.log4j.Logger;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Author: whn
 * Date: 2019-12-2 16:50
 * Version: 1.0
 * Function:
 */
public class TimeUtils {
    private static final Logger log = Logger.getLogger(TimeUtils.class);

    // 返回当前系统日期前多少天，负数表示后多少天
    public static String getDateBefore(int days) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date prevDay = null;
        try {
            Date day = sdf.parse(sdf.format(new Date()));
            long ms = day.getTime() - days * 24 * 3600 * 1000L;
            prevDay = new Date(ms);
        } catch (ParseException e) {
            log.error("get yesterday error：" + e);
            e.printStackTrace();
        }
        return sdf.format(prevDay);
    }

    // 输入(正数，日期)， 返回日期，表示该日期前多少天， 负数表示该日期后多少天
    public static String getDateBefore(int days, String dateDay) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date prevDay = null;
        try {
            Date day = sdf.parse(dateDay);
            long ms = day.getTime() - days * 24 * 3600 * 1000L;
            prevDay = new Date(ms);
        } catch (ParseException e) {
            log.error("get yesterday error：" + e);
            e.printStackTrace();
        }
        return sdf.format(prevDay);
    }

    public static void main(String[] args) {
        System.out.println(getDateBefore(-1, "2019-12-08"));
    }


    public <T> T name(T a ){
        return null ;
    }
}
