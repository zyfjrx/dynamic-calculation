package com.byt.common.utils;

import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 * @title: 计算参数处理
 * @author: zhangyifan
 * @date: 2022/7/1 13:21
 */
public class CalculateParamsUtil {

    public static void getCalAndTime(Set<String> set) {
        for (String p : set) {
            String[] ps = p.split("#");
            String calculate = ps[0].toUpperCase();
            ArrayList<Tuple3<String, String,String>> pTimeGap = new ArrayList<>();
            ArrayList<Tuple3<String, String, String>> params = new ArrayList<>();
            if (ps.length == 3) {
                // AVG_SUM#1,2#6
                String[] timeGaps = ps[1].split(",");
                for (String timeGap : timeGaps) {
                    pTimeGap.add(Tuple3.of(timeGap, ps[2],""));
                }
            } else if (ps.length > 1) {
                //1,2|2,3
                //(1,2)  (2,3)
                String[] timeGaps = ps[1].split("\\|");
                for (String timeGap : timeGaps) {
                    String[] split = timeGap.split(",");
                    pTimeGap.add(Tuple3.of(split[0], split[1],""));
                }
            } else {
                pTimeGap.add(Tuple3.of("5", "5",""));
            }

            String[] cals = calculate.split("_");
            for (int i = 0; i < cals.length; i++) {
                Tuple3<String, String,String> tuple2 = pTimeGap.get(i);
                System.out.println(tuple2.f0+","+tuple2.f1 +"---"+cals[i]);
                //params.add(Tuple3.of(cals[i], tuple2.f0, tuple2.f1));
               // System.out.println(params.get(i).toString());
            }

        }
    }



    public static void getParams(Set<String> set) {
        for (String p : set) {
            String[] ps = p.split("#");
            String calculates = ps[0].toUpperCase();
            String params = ps[1];
            String[] timeOrParams = params.split("\\|");
            String[] cals = calculates.split("_");
            for (int i = 0; i < cals.length; i++) {
                System.out.println(cals[i]+"->"+timeOrParams[i]);
            }
        }
    }




    public static void main(String[] args) {
        HashSet<String> strings = new HashSet<>();
        //strings.add("AVG_SUM#1,5|2,3");
        strings.add("AVG_nbefore#1,5|3#");
        //strings.add("AVG_SUM#1,5|6,7#");
       /* strings.add("AVGFF#12#13");
        strings.add("AVGFF#12,13#");
        strings.add("AVGFF##");*/
        getCalAndTime(strings);
        //getParams(strings);
    }
}
