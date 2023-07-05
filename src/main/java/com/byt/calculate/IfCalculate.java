package com.byt.calculate;

import com.byt.pojo.TagKafkaInfo;

/**
 * @title:
 * @author: zhangyf
 * @date: 2023/7/5 14:32
 **/
public class IfCalculate {
    public static TagKafkaInfo ifCal(TagKafkaInfo tagKafkaInfo){
        if (tagKafkaInfo.getCurrIndex() < tagKafkaInfo.getTotalIndex()){
            tagKafkaInfo.setCurrCal(tagKafkaInfo.getCalculateType().split("_")[tagKafkaInfo.getCurrIndex()]);
        } else if (tagKafkaInfo.getCurrIndex() == tagKafkaInfo.getTotalIndex()){
            tagKafkaInfo.setCurrCal("over");
        }
        return tagKafkaInfo;
    }
}
