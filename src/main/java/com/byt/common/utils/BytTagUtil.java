package com.byt.common.utils;

import com.byt.tagcalculate.pojo.TagKafkaInfo;
import com.byt.tagcalculate.pojo.TagProperties;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @title: 标签处理工具类
 * @author: zhangyifan
 * @date: 2022/8/17 10:49
 */
public class BytTagUtil {


    private static List<String> twoParamTimeCal = Arrays.asList(new String[]{"AVG", "INTERP", "VARIANCE", "STD", "MAX", "MIN", "MEDIAN", "RANGE", "CV", "SLOPE", "PSEQ", "SUM"});
    private static List<String> twoParamCal = Arrays.asList(new String[]{"KF"});
    private static List<String> dejumpParamCal = Arrays.asList(new String[]{"DEJUMP"});
    private static List<String> oneParamCal = Arrays.asList(new String[]{"TREND", "VAR", "LAST", "RAW", "EMA", "RSI"});
    private static List<String> oneParamCalFOF = Arrays.asList(new String[]{"FOF"});

    private static Integer N = 0;

    /**
     * 将标签转换为 map 方便后期处理
     *
     * @param value   原始数据list
     * @param hasTags 配置表中待计算的标签名
     * @return Map<标签名 ， 标签pojo class>
     */
    public static Map<String, TagKafkaInfo> tagInfoMap(List<TagKafkaInfo> value, Set<String> hasTags) {
        Map<String, TagKafkaInfo> tagInfoMap = new HashMap<>();
        for (TagKafkaInfo tag : value) {
            String tagName = tag.getName();
            if (hasTags.contains(tagName)) {
                tagInfoMap.put(tagName, tag);
            }
        }
        return tagInfoMap;
    }


    public static Integer getNormalState(String max, String min, BigDecimal value) {
        BigDecimal value_max = new BigDecimal(max.trim());
        BigDecimal value_min = new BigDecimal(min.trim());
        if (value_max.compareTo(value_min) == 0 || (value.compareTo(value_min) == 1 && value.compareTo(value_max) == -1)) {
            return 1;
        } else {
            return 0;
        }
    }


    /**
     * 补充字段信息核心处理类
     *
     * @param value        原始数据list
     * @param hasTags      配置表中待计算的标签名
     * @param bytInfoCache 从广播流动态获取到的配置信息（基于Flink CDC可做到实时变更实时更新）
     * @return 补充完字段的list数据
     * @throws Exception
     */
    public static List<TagKafkaInfo> bytTagData(List<TagKafkaInfo> value,
                                                Set<String> hasTags,
                                                Map<String, TagProperties> bytInfoCache, ParameterTool parameterTool) throws Exception {
        // 获取到转换为Map结构的标签信息
        Map<String, TagKafkaInfo> tagInfoMap = tagInfoMap(value, hasTags);
        // 创建list 保存处理后的数据
        List<TagKafkaInfo> bytTagData = new ArrayList<>();
        if (tagInfoMap.isEmpty()) {
            return bytTagData;
        }

        // 遍历配置信息，为原始数据补充字段
        for (Map.Entry<String, TagProperties> entry : bytInfoCache.entrySet()) {
            TagKafkaInfo bytTag = new TagKafkaInfo();
            String tagName = entry.getValue().tag_name;
            String bytName = entry.getValue().byt_name;
            String jobName = entry.getValue().task_name;
            String tagTopic = entry.getValue().tag_topic;
            Integer lineId = entry.getValue().line_id;
            String calculateType = entry.getValue().calculate_type;
            String param = entry.getValue().param;
            String op = entry.getValue().op;
            String max = entry.getValue().value_max;
            String min = entry.getValue().value_min;

            if (calculateType == null) {
                continue;
            }

            if (tagName.startsWith(parameterTool.get("formula.tag.start"))) {
                Set<String> tagSet = QlexpressUtil.getTagSet(tagName);
                try {
                    Object r = QlexpressUtil.computeExpress(tagInfoMap, tagName);
                    bytTag.setValue(new BigDecimal(r.toString()));
                    Integer normalState = getNormalState(max, min, new BigDecimal(r.toString()));
                    bytTag.setIsNormal(normalState);
                } catch (Exception e) {
                    bytTag.setValue(new BigDecimal(0));
                }

                if (tagSet.isEmpty()){
                    return bytTagData;
                }

                TagKafkaInfo originTag = tagInfoMap.get(tagSet.toArray()[0]);
                if (originTag != null && !originTag.getTopic().equals(tagTopic)) {
                    continue;
                }
                try {
                    bytTag.setTime(originTag.getTime());
                    bytTag.setTopic(originTag.getTopic());
                    bytTag.setTimestamp(originTag.getTimestamp());
                } catch (Exception e) {
                    continue;
                }
            } else {
                TagKafkaInfo originTag = tagInfoMap.get(tagName);
                if (originTag != null) {
                    if (!originTag.getTopic().equals(tagTopic)) {
                        continue;
                    }
                    Integer normalState = getNormalState(max, min, originTag.getValue());
                    bytTag.setIsNormal(normalState);
                    bytTag.setTime(originTag.getTime());
                    bytTag.setTopic(originTag.getTopic());
                    bytTag.setValue(originTag.getValue());
                    bytTag.setTimestamp(originTag.getTimestamp());
                }
            }

            bytTag.setOp(op);
            bytTag.setBytName(bytName);
            bytTag.setName(tagName);
            bytTag.setLineId(lineId);
            bytTag.setCalculateType(calculateType);
            bytTag.setCalculateParam(param);
            bytTag.setTaskName(entry.getValue().task_name);
            bytTag.setStatus(entry.getValue().status);
            if (tagInfoMap.get(tagName) != null || tagName.startsWith(parameterTool.get("formula.tag.start"))) {
                bytTagData.add(parseParams(bytTag, calculateType, param));
            }
        }
        return bytTagData;
    }

    public static TagKafkaInfo parseParams(TagKafkaInfo bytTag, String type, String param) {
        String[] types = type.split("_");
        String[] params = param.split("\\|");
        for (int i = 0; i < types.length; i++) {
            if (twoParamTimeCal.contains(types[i])) {
                String[] split = params[i].split(",");
                bytTag.setWinSize(timeParams(split[0]));
                bytTag.setWinSlide(timeParams(split[1]));
            } else if (twoParamCal.contains(types[i])) {
                String[] split = params[i].split(",");
                bytTag.setDt(Double.parseDouble(split[0]));
                bytTag.setR(Double.parseDouble(split[1]));
            } else if (dejumpParamCal.contains(types[i])) {
                String[] split = params[i].split(",");
                bytTag.setLowerInt(Double.parseDouble(split[0]));
                bytTag.setUpperInt(Double.parseDouble(split[1]));
            } else if (oneParamCal.contains(types[i])) {
                String[] split = params[i].split(",");
                bytTag.setCurrNBefore(Integer.parseInt(split[0]));
            } else {
                String[] split = params[i].split(",");
                bytTag.setA(Double.parseDouble(split[0]));
            }
        }
        bytTag.setTotalIndex(types.length);
        bytTag.setCurrIndex(0);
        bytTag.setCurrCal(types[0]);
        return bytTag;
    }


    public static void outputByWindow(TagKafkaInfo tagKafkaInfo, ProcessWindowFunction<TagKafkaInfo, TagKafkaInfo, String, TimeWindow>.Context context, Collector<TagKafkaInfo> out, OutputTag<TagKafkaInfo> dwdOutPutTag) {
        tagKafkaInfo.setTimestamp(context.window().getEnd());
        tagKafkaInfo.setCurrIndex(tagKafkaInfo.getCurrIndex() + 1);
        if (tagKafkaInfo.getCurrIndex() < tagKafkaInfo.getTotalIndex()) {
            tagKafkaInfo.setCurrCal(tagKafkaInfo.getCalculateType().split("_")[tagKafkaInfo.getCurrIndex()]);
            context.output(dwdOutPutTag, tagKafkaInfo);
        } else if (tagKafkaInfo.getCurrIndex() == tagKafkaInfo.getTotalIndex()) {
            tagKafkaInfo.setCurrCal("over");
            out.collect(tagKafkaInfo);
        }
    }


    public static void outputByKeyed(TagKafkaInfo tagKafkaInfo, KeyedProcessFunction<String, TagKafkaInfo, TagKafkaInfo>.Context context, Collector<TagKafkaInfo> out, OutputTag<TagKafkaInfo> dwdOutPutTag) {
        tagKafkaInfo.setCurrIndex(tagKafkaInfo.getCurrIndex() + 1);
        if (tagKafkaInfo.getCurrIndex() < tagKafkaInfo.getTotalIndex()) {
            tagKafkaInfo.setCurrCal(tagKafkaInfo.getCalculateType().split("_")[tagKafkaInfo.getCurrIndex()]);
            context.output(dwdOutPutTag, tagKafkaInfo);
        } else if (tagKafkaInfo.getCurrIndex() == tagKafkaInfo.getTotalIndex()) {
            tagKafkaInfo.setCurrCal("over");
            out.collect(tagKafkaInfo);
        }
    }

    public static void outputByKeyed(TagKafkaInfo tagKafkaInfo, PatternProcessFunction.Context context, Collector<TagKafkaInfo> out, OutputTag<TagKafkaInfo> dwdOutPutTag) {
        tagKafkaInfo.setCurrIndex(tagKafkaInfo.getCurrIndex() + 1);
        if (tagKafkaInfo.getCurrIndex() < tagKafkaInfo.getTotalIndex()) {
            tagKafkaInfo.setCurrCal(tagKafkaInfo.getCalculateType().split("_")[tagKafkaInfo.getCurrIndex()]);
            context.output(dwdOutPutTag, tagKafkaInfo);
        } else if (tagKafkaInfo.getCurrIndex() == tagKafkaInfo.getTotalIndex()) {
            tagKafkaInfo.setCurrCal("over");
            out.collect(tagKafkaInfo);
        }
    }


    public static String reformat(long l) {
        //yyyyMMddHHmmss
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//可以方便地修改日期格式
        String s1 = "";
        try {
            s1 = sdf.format(new Date(l));

        } catch (Exception e) {
            System.out.println(e);
        }
        return s1;
    }


    public static String formatTime(String srcTime) {
        SimpleDateFormat srcSdf = new SimpleDateFormat("yyyyMMddHHmmss");
        SimpleDateFormat tarSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String targetTime = "";
        try {
            long time = srcSdf.parse(srcTime).getTime();
            targetTime = tarSdf.format(time);
        } catch (Exception e) {
            System.out.println(e + "时间格式转换异常");
        }
        return targetTime;
    }


    public static Long timeParams(String str) {
        Long time = null;
        if (str.contains("s")) {
            time = Long.parseLong(str.replace("s", "")) * 1000L;
        } else if (str.contains("m")) {
            time = Long.parseLong(str.replace("m", "")) * 60L * 1000L;
        }
        return time;
    }


}
