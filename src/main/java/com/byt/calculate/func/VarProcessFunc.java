package com.byt.calculate.func;

import com.byt.pojo.TagKafkaInfo;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.math.BigDecimal;
import java.util.LinkedList;
import java.util.Queue;

/**
 * @title: VAR算子函数
 * @author: zhangyf
 * @date: 2023/7/5 14:04
 **/
public class VarProcessFunc extends KeyedProcessFunction<String, TagKafkaInfo, TagKafkaInfo> {
    private OutputTag<TagKafkaInfo> dwdOutPutTag;
    private transient Queue<TagKafkaInfo> lastQueue;

    public VarProcessFunc(OutputTag<TagKafkaInfo> dwdOutPutTag) {
        this.dwdOutPutTag = dwdOutPutTag;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        lastQueue = new LinkedList<>();
    }

    @Override
    public void processElement(TagKafkaInfo value, KeyedProcessFunction<String, TagKafkaInfo, TagKafkaInfo>.Context ctx, Collector<TagKafkaInfo> out) throws Exception {
        Integer nBefore = value.getN();
        lastQueue.offer(value);
        int size = lastQueue.size();
        if (size > nBefore) {
            TagKafkaInfo firstTag = lastQueue.poll();
            BigDecimal firstTagValue = firstTag.getValue();
            TagKafkaInfo newTag = new TagKafkaInfo();
            BeanUtils.copyProperties(newTag, value);
            try {
                BigDecimal diffValue = value.getValue().subtract(firstTagValue);
                newTag.setValue(diffValue);
            } catch (Exception e) {
                newTag.setValue(null);
                e.printStackTrace();
            }
            newTag.setCurrIndex(value.getCurrIndex() + 1);
            if (newTag.getCurrIndex() < newTag.getTotalIndex()) {
                // 还需要进行后续计算
                String[] split = value.getCalculateType().split("_");
                newTag.setCurrCal(split[newTag.getCurrIndex()]);
                ctx.output(dwdOutPutTag, newTag);
            } else {
                // 计算完成输出
                newTag.setCurrCal("over");
                out.collect(newTag);
            }
        }
    }
}
