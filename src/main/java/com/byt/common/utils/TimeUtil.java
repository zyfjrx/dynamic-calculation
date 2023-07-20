package com.byt.common.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

public class TimeUtil {

    public static long getTimeStamp() {
        Date now = new Date();
        Date now_5 = new Date(now.getTime() - 60000 * 5); //5分钟前的时间
        return now_5.getTime();
    }

    public static String getCurrentTimeStr() {
        SimpleDateFormat sdf = new SimpleDateFormat();// 格式化时间
//        sdf.applyPattern("yyyyMMddHHmmss");// a为am/pm的标记
        sdf.applyPattern("yyyy-MM-dd HH:mm:ss");// a为am/pm的标记
        Date date = new Date();// 获取当前时间
        return sdf.format(date);
    }

    public static long getStartTime(String s) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//可以方便地修改日期格式
        long st = 0;
        try {
            st = dateFormat.parse(s).getTime();
        } catch (Exception e) {
            System.out.println(e);
        }
        return st;
    }

    public static String reformat(String s) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//可以方便地修改日期格式
        String s1 = "";
        try {
            long st = dateFormat.parse(s).getTime();
            SimpleDateFormat sdf = new SimpleDateFormat();// 格式化时间
            sdf.applyPattern("yyyyMMddHHmmss");// a为am/pm的标记
            s1 = sdf.format(st);

        } catch (Exception e) {
            System.out.println(e);
        }
        return s1;
    }


    public static Long toTimeMillis(String str) {
        Long millis = null;
        if (str.contains("h")) {
            Long nh = Long.parseLong(str.replace("h", ""));
            millis = nh * 60L * 60L * 1000L;

        } else if (str.contains("m")) {
            Long nm = Long.parseLong(str.replace("m", ""));
            millis = nm * 60L * 1000L;
        } else {
            Long ns = Long.parseLong(str.replace("s", ""));
            millis = ns * 1000L;
        }
        return millis;

    }



    public static String getCurrentTimeString() {
        SimpleDateFormat sdf = new SimpleDateFormat();// 格式化时间
//        sdf.applyPattern("yyyyMMddHHmmss");// a为am/pm的标记
        sdf.applyPattern("yyyy-MM-dd HH:mm:ss");// a为am/pm的标记
        Date date = new Date();// 获取当前时间
        return sdf.format(date);
    }

}
