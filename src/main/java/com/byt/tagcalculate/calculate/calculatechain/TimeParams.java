package com.byt.tagcalculate.calculate.calculatechain;

import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @title: 时间处理
 * @author: zhangyifan
 * @date: 2022/8/29 10:52
 */
public class TimeParams {
    public static Time timeParams(String tStr) {

        if (tStr.contains("d")) {
            return Time.days(Long.parseLong(tStr.replace("d", "")));
        } else if (tStr.contains("h")) {
            return Time.hours(Long.parseLong(tStr.replace("h", "")));
        }else if (tStr.contains("m")){
            return Time.minutes(Long.parseLong(tStr.replace("m","")));
        } else {
            return Time.seconds(Long.parseLong(tStr.replace("s","")));
        }
    }
}
