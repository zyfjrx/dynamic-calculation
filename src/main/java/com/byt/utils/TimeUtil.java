package com.byt.utils;

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

//    public static void main(String[] args) {
//        long l = getStartTime("20200830202000");
//        System.out.println(l);
//        System.out.println(getCurrentTimeStr());
//        Timestamp t = Timestamp.valueOf("2021-09-27 10:11:00");
//        System.out.println(getStartTime("2021-09-27 10:11:00"));
//    }

}
