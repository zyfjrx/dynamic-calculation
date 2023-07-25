package com.byt.common.utils;

import com.byt.tagcalculate.constants.PropertiesConstants;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * @title:
 * @author: zhangyifan
 * @date: 2022/8/6 15:17
 */
public class JobUtils {
    public static List<String> getSourceTopicWithJobs(String jobs,String lineId) {
        String[] jobArr = jobs.split(",");
        HashSet<String> hashSet = new HashSet<>();
        for (String s : jobArr) {
            hashSet.add(ConfigManager.getProperty(PropertiesConstants.KAFKA_DWD_TOPIC) + lineId+"_"+ s);
        }
        System.out.println(hashSet);
        return new ArrayList<>(hashSet);
    }

    public static String geJobName(String jobs) {
        String[] jobarry = jobs.split(",");
        StringBuilder sql = new StringBuilder();
        for (int i = 0; i < jobarry.length; i++) {
            sql.append(jobarry[i]);
            if (i < jobarry.length - 1) {
                sql.append("_");
            }
        }
        return sql.toString();
    }


    public static void main(String[] args) {
        System.out.println(geJobName("job1"));
    }
}
