package com.byt.utils;

import com.byt.pojo.TagKafkaInfo;
import com.byt.pojo.TagProperties;
import java.math.BigDecimal;
import java.util.*;

/**
 * @title: 标签处理工具类
 * @author: zhangyifan
 * @date: 2022/8/17 10:49
 */
public class BytTagUtil {


    private static List<String> twoParamTimeCal = Arrays.asList(new String[]{"AVG", "INTERP", "VARIANCE", "STD", "MAX", "MIN", "MEDIAN", "RANGE", "CV", "SLOPE", "PSEQ"});
    private static List<String> twoParamCal = Arrays.asList(new String[]{"KF"});
    private static List<String> oneParamCal = Arrays.asList(new String[]{"TREND", "VAR", "LAST", "RAW"});
    private static List<String> oneParamCalFOF = Arrays.asList(new String[]{"FOF"});


    /**
     * 将标签转换为 map 方便后期处理
     * @param value 原始数据list
     * @param hasTags 配置表中待计算的标签名
     * @return Map<标签名， 标签pojo class>
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

    /**
     * 根据数据相关参数判断数据是否正常
     * @param bytInfoCache 从广播流动态获取到的配置信息（基于Flink CDC可做到实时变更实时更新）
     * @param tagInfoMap  Map<标签名， 标签pojo class>
     * @param bytName 输出结果名字
     * @param tagName 标签名字
     * @return normal or abnormal
     */
    public static String getNormalState(Map<String, TagProperties> bytInfoCache,
                                        Map<String, TagKafkaInfo> tagInfoMap,
                                        String bytName, String tagName) {
        if (!bytInfoCache.containsKey(bytName)) {
            return "normal";
        }
        BigDecimal max = new BigDecimal(bytInfoCache.get(bytName).value_max);
        BigDecimal min = new BigDecimal(bytInfoCache.get(bytName).value_min);
        if (max.compareTo(min) == 0) {
            return "normal";
        }
        BigDecimal v = tagInfoMap.get(tagName).getValue();
        if (v.compareTo(max) == 1 || v.compareTo(min) == -1) {
            return "abnormal";
        } else {
            return "normal";
        }
    }


    /**
     * 补充字段信息核心处理类
     * @param value 原始数据list
     * @param hasTags 配置表中待计算的标签名
     * @param bytInfoCache 从广播流动态获取到的配置信息（基于Flink CDC可做到实时变更实时更新）
     * @return 补充完字段的list数据
     * @throws Exception
     */
    public static List<TagKafkaInfo> bytTagData(List<TagKafkaInfo> value,
                                                Set<String> hasTags,
                                                Map<String, TagProperties> bytInfoCache) throws Exception {
        // 获取到转换为Map结构的标签信息
        Map<String, TagKafkaInfo> tagInfoMap = tagInfoMap(value, hasTags);
        // 创建list 保存处理后的数据
        List<TagKafkaInfo> bytTagData = new ArrayList<>();
        String normalState = "normal";
        if (tagInfoMap.isEmpty()) {
            return bytTagData;
        }

        // 遍历配置信息，为原始数据补充字段
        for (Map.Entry<String, TagProperties> entry : bytInfoCache.entrySet()) {
            TagKafkaInfo bytTag = new TagKafkaInfo();
            String tagName = entry.getValue().tag_name;
            String bytName = entry.getValue().byt_name;
            String jobName = entry.getValue().task_name;
            // dev
            String tagTopic = entry.getValue().tag_topic;
            Integer lineId = entry.getValue().line_id;
            String calculateType = entry.getValue().calculate_type;

            if (calculateType == null) {
                continue;
            }

            if (tagName.contains(FormulaTag.START)) {
                Set<String> tagSet = QlexpressUtil.getTagSet(tagName);
                for (String t : tagSet) {
                    String formulaTag = bytName + "_" + t;
                    normalState = getNormalState(bytInfoCache, tagInfoMap, formulaTag + jobName, t);
                }
                if (normalState.equals("normal")) {
                    try {
                        Object r = QlexpressUtil.computeExpress(tagInfoMap, tagName);
                        bytTag.setValue(new BigDecimal(r.toString()));
                    } catch (Exception e) {
                        bytTag.setValue(new BigDecimal(0));
                    }
                }
                TagKafkaInfo originTag = tagInfoMap.get(tagSet.toArray()[0]);
                if (originTag != null && !originTag.getTopic().equals(tagTopic)) {
                    continue;
                }
                try {
                    bytTag.setTime(originTag.getTime());
                    // todo dev
                    bytTag.setTopic(originTag.getTopic());
                    bytTag.setTimestamp(originTag.getTimestamp());
                } catch (Exception e) {
//                    System.out.println("Error: " + tagName + " " + originTag + e);
                    continue;
                }
            } else {
                TagKafkaInfo originTag = tagInfoMap.get(tagName);
                if (originTag != null) {
                    if (!originTag.getTopic().equals(tagTopic)){
                        continue;
                    }
                    normalState = getNormalState(bytInfoCache, tagInfoMap, bytName + jobName, tagName);
                    bytTag.setTime(originTag.getTime());
                    // todo dev
                    bytTag.setTopic(originTag.getTopic());
                    bytTag.setValue(originTag.getValue());
                    bytTag.setTimestamp(originTag.getTimestamp());
                }
            }

            bytTag.setIsNormal(normalState.equals("normal") ? 1 : 0);
            bytTag.setBytName(bytName);
            bytTag.setName(tagName);
            // dev

            bytTag.setLineId(lineId);
            bytTag.setCalculateType(calculateType);
            //bytTag.setTopic( entry.getValue().tag_topic);
            bytTag.setCalculateParam( entry.getValue().param);
            //bytTag.setSlideGap( entry.getValue().slide_gap);
            bytTag.setTaskName(entry.getValue().task_name);

            System.out.println(bytTag+"===========================");
            if (tagInfoMap.get(tagName) != null || tagName.contains(FormulaTag.START)) {
                bytTagData.add(parseParams(bytTag,calculateType,entry.getValue().param));
            }
        }
        if (normalState.equals("abnormal")) {
            bytTagData.get(0).setIsNormal(0);
        }
        return bytTagData;
    }

    public static TagKafkaInfo parseParams(TagKafkaInfo bytTag,String type,String param){
        String[] types = type.split("_");
        String[] params = param.split("\\|");
        for (int i = 0; i < types.length; i++) {
            if (twoParamTimeCal.contains(types[i])){
                String[] split = params[i].split(",");
                bytTag.setWinSize(split[0]);
                bytTag.setWinSlide(split[1]);
            } else if (twoParamCal.contains(types[i])){
                String[] split = params[i].split(",");
                bytTag.setDt(Double.parseDouble(split[0]));
                bytTag.setR(Double.parseDouble(split[1]));
            }else if (oneParamCal.contains(types[i])){
                String[] split = params[i].split(",");
                bytTag.setN(Integer.parseInt(split[0]));
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
}
