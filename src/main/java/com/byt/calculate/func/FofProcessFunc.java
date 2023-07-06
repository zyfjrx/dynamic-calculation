package com.byt.calculate.func;

import com.byt.calculate.TStream;
import com.byt.pojo.TagKafkaInfo;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;

/**
 * @title: 一阶低通滤波算法
 * @author: zhangyifan
 * @date: 2022/8/28 13:47
 */
public class FofProcessFunc extends KeyedProcessFunction<String,TagKafkaInfo, TagKafkaInfo> {
    private ValueState<BigDecimal> lastFirstOrder;
    private SimpleDateFormat sdf;
    private OutputTag<TagKafkaInfo> dwdOutPutTag;

    public FofProcessFunc(OutputTag<TagKafkaInfo> dwdOutPutTag) {
        this.dwdOutPutTag = dwdOutPutTag;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        lastFirstOrder = getRuntimeContext().getState(
                new ValueStateDescriptor<BigDecimal>(
                        "firstOrder",
                        Types.BIG_DEC
                )
        );
        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    }

    @Override
    public void processElement(TagKafkaInfo tagKafkaInfo, KeyedProcessFunction<String, TagKafkaInfo, TagKafkaInfo>.Context ctx, Collector<TagKafkaInfo> out) throws Exception {
        BigDecimal a = new BigDecimal(tagKafkaInfo.getA());
        if (lastFirstOrder.value() == null) {
            lastFirstOrder.update(tagKafkaInfo.getValue());
        }
        lastFirstOrder.update(
                a.multiply(tagKafkaInfo.getValue())
                        .add(
                                BigDecimal.ONE.subtract(a).multiply(lastFirstOrder.value())
                        )
        );
        tagKafkaInfo.setValue(lastFirstOrder.value().setScale(4, BigDecimal.ROUND_HALF_UP));
        tagKafkaInfo.setCurrIndex(tagKafkaInfo.getCurrIndex() + 1);
        if (tagKafkaInfo.getCurrIndex() < tagKafkaInfo.getTotalIndex()){
            tagKafkaInfo.setCurrCal(tagKafkaInfo.getCalculateType().split("_")[tagKafkaInfo.getCurrIndex()]);
            ctx.output(dwdOutPutTag,tagKafkaInfo);
        } else if (tagKafkaInfo.getCurrIndex() == tagKafkaInfo.getTotalIndex()){
            tagKafkaInfo.setCurrCal("over");
            out.collect(tagKafkaInfo);
        }
        // TODO dev
        //tagKafkaInfo.setTime(sdf.format(new Timestamp(System.currentTimeMillis())));
        out.collect(tagKafkaInfo);
    }
}
