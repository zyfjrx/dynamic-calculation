package com.byt.tagcalculate.calculate.func;

import com.byt.common.utils.BytTagUtil;
import com.byt.tagcalculate.pojo.TagKafkaInfo;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * @title: EMA算子函数
 * @author: zhangyf
 * @date: 2023/7/24 14:04
 **/
public class EmaProcessFunc extends KeyedProcessFunction<String, TagKafkaInfo, TagKafkaInfo> {
    private  OutputTag<TagKafkaInfo> dwdOutPutTag;
    private ValueState<BigDecimal> emaState;
    private BigDecimal EMA;

    @Override
    public void open(Configuration parameters) throws Exception {
        emaState = getRuntimeContext().getState(
                new ValueStateDescriptor<BigDecimal>("ema",Types.BIG_DEC)
        );
    }
    public EmaProcessFunc(OutputTag<TagKafkaInfo> dwdOutPutTag) {
        this.dwdOutPutTag = dwdOutPutTag;
    }


    @Override
    public void processElement(TagKafkaInfo tagKafkaInfo, KeyedProcessFunction<String, TagKafkaInfo, TagKafkaInfo>.Context ctx, Collector<TagKafkaInfo> out) throws Exception {
        Integer nBefore = tagKafkaInfo.getCurrNBefore();
        BigDecimal lastEma = emaState.value();
        BigDecimal value = tagKafkaInfo.getValue();
        if (lastEma == null){
            emaState.update(value);
        } else {
            EMA = (value.multiply(BigDecimal.valueOf(2)).add(lastEma.multiply(BigDecimal.valueOf(nBefore-1)))).divide(BigDecimal.valueOf(nBefore+1),8, RoundingMode.HALF_UP);
            emaState.update(EMA);
        }
        tagKafkaInfo.setValue(emaState.value());
        BytTagUtil.outputByKeyed(tagKafkaInfo,ctx,out,dwdOutPutTag);
    }
}
