package com.byt.tagcalculate.calculate.func;

import com.byt.common.utils.BytTagUtil;
import com.byt.tagcalculate.pojo.TagKafkaInfo;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.math.BigDecimal;
import java.util.LinkedList;
import java.util.Queue;

/**
 * @title: RSI算子函数
 * @author: zhangyf
 * @date: 2023/7/24 14:04
 **/
public class RsiProcessFunc extends KeyedProcessFunction<String, TagKafkaInfo, TagKafkaInfo> {
    private OutputTag<TagKafkaInfo> dwdOutPutTag;
    private MapState<String, Queue<Double>> gainMapState; // 上涨幅度列表
    private MapState<String, Queue<Double>> lossMapState; // 下跌幅度列表
    private ValueState<Double> prevClosePrice; // 前一日收盘价

    @Override
    public void open(Configuration parameters) throws Exception {
        gainMapState = getRuntimeContext().getMapState(
                new MapStateDescriptor<String, Queue<Double>>(
                        "gainMapState",
                        Types.STRING,
                        TypeInformation.of(new TypeHint<Queue<Double>>() {
                        })
                )
        );
        lossMapState = getRuntimeContext().getMapState(
                new MapStateDescriptor<String, Queue<Double>>(
                        "lossMapState",
                        Types.STRING,
                        TypeInformation.of(new TypeHint<Queue<Double>>() {
                        })
                )
        );

        prevClosePrice = getRuntimeContext().getState(
                new ValueStateDescriptor<Double>(
                        "prevClosePrice",
                        Double.class
                )
        );
    }

    public RsiProcessFunc(OutputTag<TagKafkaInfo> dwdOutPutTag) {
        this.dwdOutPutTag = dwdOutPutTag;
    }


    @Override
    public void processElement(TagKafkaInfo tagKafkaInfo, KeyedProcessFunction<String, TagKafkaInfo, TagKafkaInfo>.Context ctx, Collector<TagKafkaInfo> out) throws Exception {
        Integer nBefore = tagKafkaInfo.getCurrNBefore();
        String key = tagKafkaInfo.getBytName();
        if (prevClosePrice.value() == null) {
            if (!gainMapState.contains(key)) {
                Queue<Double> queue = new LinkedList<>();
                queue.offer(0.0);
                gainMapState.put(key, queue);
            } else {
                gainMapState.get(key).offer(0.0);
            }

            if (!lossMapState.contains(key)) {
                Queue<Double> queue = new LinkedList<>();
                queue.offer(0.0);
                lossMapState.put(key, queue);
            } else {
                gainMapState.get(key).offer(0.0);
            }
        } else {
            double gain = Math.max(tagKafkaInfo.getValue().doubleValue() - prevClosePrice.value(), 0);
            double loss = Math.max(prevClosePrice.value() - tagKafkaInfo.getValue().doubleValue(), 0);
            gainMapState.get(key).offer(gain);
            lossMapState.get(key).offer(loss);
        }
        prevClosePrice.update(tagKafkaInfo.getValue().doubleValue());
        Queue<Double> gainQueue = gainMapState.get(key);
        Queue<Double> lossQueue = lossMapState.get(key);
        if (gainQueue.size() > nBefore - 1 && lossQueue.size() > nBefore - 1) {
            double avgGain = gainQueue.stream().mapToDouble(Double::doubleValue).average().getAsDouble();
            double avgLoss = lossQueue.stream().mapToDouble(Double::doubleValue).average().getAsDouble();
            double rs = avgGain / avgLoss;
            double rsi = 100 - (100 / (1 + rs));
            gainQueue.poll();
            lossQueue.poll();
            tagKafkaInfo.setValue(BigDecimal.valueOf(rsi).setScale(4, BigDecimal.ROUND_HALF_UP));
            BytTagUtil.outputByKeyed(tagKafkaInfo,ctx,out,dwdOutPutTag);
        }
    }
}