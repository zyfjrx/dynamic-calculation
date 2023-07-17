package com.byt.tagcalculate.calculate.func;

import com.byt.tagcalculate.pojo.TagKafkaInfo;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @title: CEP 预处理函数
 * @author: zhangyf
 * @date: 2023/7/17 11:59
 **/
public class CepProcessFunc extends KeyedProcessFunction<String, TagKafkaInfo, TagKafkaInfo> {
    private ValueState<Integer> valueState;

    @Override
    public void open(Configuration parameters) throws Exception {
        valueState = getRuntimeContext().getState(
                new ValueStateDescriptor<Integer>(
                        "intstate",
                        Types.INT
                )
        );
    }

    @Override
    public void processElement(TagKafkaInfo value, KeyedProcessFunction<String, TagKafkaInfo, TagKafkaInfo>.Context ctx, Collector<TagKafkaInfo> out) throws Exception {
        Integer lastValue = valueState.value();
        Integer currNBefore = value.getCurrNBefore();
        if (lastValue == null ) {
            // 第一条数据来的时候处理逻辑
            valueState.update(currNBefore);
            value.setChangeNFA(true);
        } else {
            // 第二条及以后数据到来时候的处理逻辑
            if (lastValue == value.getCurrNBefore()){
                value.setChangeNFA(false);
            } else {
                valueState.update(currNBefore);
                value.setChangeNFA(true);
            }
        }
        //System.out.println("*****************:"+value);
        out.collect(value);
    }
}
