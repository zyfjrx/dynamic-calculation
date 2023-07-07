package com.byt.common.utils;

import com.byt.tagcalculate.pojo.TagKafkaInfo;
import com.google.common.collect.Sets;
import org.apache.flink.util.OutputTag;

import java.util.*;

/**
 * @title:
 * @author: zhangyifan
 * @date: 2022/7/4 15:07
 */
public class SideOutPutTagUtil {

    private static List<String> twoParamTimeCal = Arrays.asList(new String[]{"AVG", "INTERP", "VARIANCE", "STD", "MAX", "MIN", "MEDIAN", "RANGE", "CV", "SLOPE", "PSEQ"});
    private static List<String> twoParamCal = Arrays.asList(new String[]{"KF","DEJUMP"});
    private static List<String> oneParamCal = Arrays.asList(new String[]{"TREND", "VAR", "LAST", "FOF", "RAW"});


    public static Set<String> getSideParams(String jobName) {
        Set<String> params = QueryDbUtil.getParams(jobName);
        return params;
    }

    public static Set<String> getSideParamsWithJobs(String jobNames) {
        Set<String> inParams = QueryDbUtil.getParamsWithJobs(jobNames);
        Set<String> outParams = checkParams(QueryDbUtil.getParamsWithJobs(jobNames));
        if (inParams.size() != outParams.size()) {
            System.out.println("warningMsg:" + Sets.difference(inParams, outParams) + "，参数异常被过滤");
        }
        return outParams;
    }

    public static HashMap<String, OutputTag<TagKafkaInfo>> getSideOutPutTags(Set<String> sideParams) {
        //Set<String> sideParams = getSideParams(taskName);
        HashMap<String, OutputTag<TagKafkaInfo>> sideOutputTags = new HashMap<>();
        if (sideParams.size() > 0) {
            for (String sideParam : sideParams) {
                sideOutputTags.put(sideParam, new OutputTag<TagKafkaInfo>(sideParam) {
                });
            }
        }
        return sideOutputTags;
    }


    public static HashMap<String, OutputTag<TagKafkaInfo>> getSideOutPutTags() {
        HashMap<String, OutputTag<TagKafkaInfo>> sideOutputTags = new HashMap<>();
        for (String side : twoParamTimeCal) {
            sideOutputTags.put(side,new OutputTag<TagKafkaInfo>(side){});
        }
        for (String side : twoParamCal) {
            sideOutputTags.put(side,new OutputTag<TagKafkaInfo>(side){});
        }
        for (String side : oneParamCal) {
            sideOutputTags.put(side,new OutputTag<TagKafkaInfo>(side){});
        }
        return sideOutputTags;
    }



    /**
     * 过滤不规则参数
     *
     * @param inParams
     * @return
     */
    public static Set<String> checkParams(Set<String> inParams) {
        Set<String> outParams = new HashSet<>();
        f1:
        for (String inParam : inParams) {
            String[] fields = inParam.split("#");
            if (fields.length < 2) continue;
            String[] cals = fields[0].split("_");
            String[] params = fields[1].split("\\|");
            if (cals.length != params.length) continue;
            f2:
            for (int i = 0; i < cals.length; i++) {
                if (twoParamTimeCal.contains(cals[i])) {
                    String[] times = params[i].split(",");
                    /**
                     * 1.时间类型算子必须为两个参数
                     * 2.时间必须大于0
                     * 3.窗口长度必须大于等于滑动步长
                     */
                    if (times.length != 2) continue f1;
                    if (replaceStr(times[0]) <= 0 || replaceStr(times[1]) <= 0) continue f1;
                    if (toSecond(times[0]) < toSecond(times[1])) continue f1;
                } else if (twoParamCal.contains(cals[i])) {
                    String[] times = params[i].split(",");
                    if (times.length != 2) continue f1;
                    if (times[i].contains("m") || times[i].contains("s")) continue f1;
                }
            }
            outParams.add(inParam);
        }
        return outParams;
    }

    /**
     * in: 100s string
     * out: 100 int
     * @param timer
     * @return
     */
    public static Integer replaceStr(String timer) {
        String result = null;
        if (timer.contains("s") || timer.contains("m")) {
            result = timer.replace(timer.substring(timer.length() - 1), "");
        }
        return Integer.parseInt(result);
    }

    public static Long toSecond(String timer){
        Long second = null;
        if (timer.contains("m")){
          second =  Long.parseLong(timer.replace("m","")) * 60L;
        }else if (timer.contains("s")){
            second = Long.parseLong(timer.replace("s",""));
        }else if (timer.contains("h")){
            second = Long.parseLong(timer.replace("h","")) * 60L * 60L;
        }
        return second;

    }


    public static void main(String[] args) {
//        HashSet<String> strings = new HashSet<>();
//        strings.add("AVG_KF_MAX#5s,5s|1m,1m|3s,2s");
//        strings.add("MAX#5s,");
//        strings.add("PSEQ#100s,50s");
//        strings.add("KF#0.001,10000");
//        strings.add("KF#5s,5s");
//        strings.add("MIN#");
//        strings.add("AVG#1m,7s");
//        strings.add("AVG_KF_RAW_KF#5m,5m|1,|N|5m,5m");
        HashMap<String, OutputTag<TagKafkaInfo>> sideOutPutTags = getSideOutPutTags();
        Iterator<String> iterator = sideOutPutTags.keySet().iterator();
        while (iterator.hasNext()) {
            String key = iterator.next();
            System.out.println(key+"----->"+sideOutPutTags.get(key));
        }
        //System.out.println(replaceStr("1000s"));
        //System.out.println("10s".split("s"));
    }
}
