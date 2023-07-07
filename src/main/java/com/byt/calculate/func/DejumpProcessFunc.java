package com.byt.calculate.func;

import com.byt.pojo.TagKafkaInfo;
import com.byt.utils.BytTagUtil;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.math.BigDecimal;
import java.util.LinkedList;
import java.util.Queue;

/**
 * @title: DEJUMP算子函数 跳变抑制-当前时刻与上一时刻相减的值如果超出（lower_int, upper_int）范围，取上个时刻的值作为当前时刻的值。
 * @author: zhangyf
 * @date: 2023/7/6 13:04
 **/
public class DejumpProcessFunc extends KeyedProcessFunction<String, TagKafkaInfo, TagKafkaInfo> {
    private OutputTag<TagKafkaInfo> dwdOutPutTag;
    private ValueState<BigDecimal> lastValue;

    public DejumpProcessFunc(OutputTag<TagKafkaInfo> dwdOutPutTag) {
        this.dwdOutPutTag = dwdOutPutTag;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        lastValue = getRuntimeContext().getState(
                new ValueStateDescriptor<BigDecimal>(
                        "lastValue",
                        Types.BIG_DEC
                )
        );
    }

    @Override
    public void processElement(TagKafkaInfo tagKafkaInfo, KeyedProcessFunction<String, TagKafkaInfo, TagKafkaInfo>.Context ctx, Collector<TagKafkaInfo> out) throws Exception {
        BigDecimal upperInt = new BigDecimal(tagKafkaInfo.getUpperInt());
        BigDecimal lowerInt = new BigDecimal(tagKafkaInfo.getLowerInt());
        if (lastValue.value() != null){
            BigDecimal jumpValue = tagKafkaInfo.getValue().subtract(lastValue.value());
            if (jumpValue.compareTo(lowerInt) == -1 || jumpValue.compareTo(upperInt) == 1){
                tagKafkaInfo.setValue(lastValue.value());
            } else {
                lastValue.update(tagKafkaInfo.getValue());
            }
        } else {
            lastValue.update(tagKafkaInfo.getValue());
        }
        BytTagUtil.outputByKeyed(tagKafkaInfo,ctx,out,dwdOutPutTag);
    }
}
