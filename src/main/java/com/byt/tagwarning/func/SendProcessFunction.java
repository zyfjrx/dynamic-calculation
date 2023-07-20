package com.byt.tagwarning.func;

import com.alibaba.fastjson.JSONObject;
import com.byt.common.utils.TimeUtil;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @title: process函数，限流
 * @author: zhangyf
 * @date: 2023/6/13 10:31
 **/
public class SendProcessFunction extends KeyedProcessFunction<String,Tuple2<String,String>,String> {
    private ValueState<String> valueState;
    @Override
    public void open(Configuration parameters) throws Exception {
        valueState = getRuntimeContext().getState(
                new ValueStateDescriptor<String>(
                        "value-state",
                        Types.STRING
                )
        );
    }


    @Override
    public void processElement(Tuple2<String, String> value, KeyedProcessFunction<String, Tuple2<String, String>, String>.Context ctx, Collector<String> out) throws Exception {
        String lastKey = valueState.value();
        // 注册一个第一条报警信息后nh的定时器
        JSONObject jsonObject = JSONObject.parseObject(value.f1);
        Long ts = jsonObject.getLong("ts");
        String period = jsonObject.getString("send_period");
        Long timeTs = ts + TimeUtil.toTimeMillis(period);

       // System.out.println("regis timer");
        // 限流做到一定时间内不重复报警
        if (lastKey == null) {
            valueState.update(value.f0);
            out.collect(value.f1);
            ctx.timerService().registerProcessingTimeTimer(timeTs);
        } else {
            if (!lastKey.equals(value.f0)) {
                // 更新key
                valueState.update(value.f0);
                out.collect(value.f1);
                ctx.timerService().registerProcessingTimeTimer(timeTs);
            }
        }


    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<String, Tuple2<String, String>, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
        valueState.clear();
        System.out.println("state clear");
    }
}
