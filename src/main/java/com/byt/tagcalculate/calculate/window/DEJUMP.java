package com.byt.tagcalculate.calculate.window;

import com.byt.tagcalculate.calculate.TStream;
import com.byt.tagcalculate.calculate.Transform;
import com.byt.tagcalculate.calculate.TransformChain;
import com.byt.tagcalculate.constants.PropertiesConstants;
import com.byt.tagcalculate.pojo.TagKafkaInfo;
import com.byt.common.utils.ConfigManager;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.util.List;

/**
 * @title: 跳变抑制-当前时刻与上一时刻相减的值如果超出（lower_int, upper_int）范围，
 * 例（-3，3），取上个时刻的值作为当前时刻的值。
 * @author: zhangyifan
 * @date: 2022/9/6 09:22
 */
public class DEJUMP implements Transform {

    private String lower;
    private String upper;

    public DEJUMP(List<String> params) {
        this.lower = params.get(0);
        this.upper = params.get(1);
    }

    public DataStream<TagKafkaInfo> deJump(DataStream<TagKafkaInfo> in, BigDecimal lower, BigDecimal upper) {
        return in
                .keyBy(new KeySelector<TagKafkaInfo, Tuple3<String,String, Integer>>() {
                    @Override
                    public Tuple3<String,String, Integer> getKey(TagKafkaInfo tagKafkaInfo) throws Exception {
                        return Tuple3.of(
                                tagKafkaInfo.getTopic(),
                                tagKafkaInfo.getBytName(),
                                tagKafkaInfo.getLineId()
                        );
                    }
                })
                .process(new KeyedProcessFunction<Tuple3<String,String, Integer>, TagKafkaInfo, TagKafkaInfo>() {
                    private ValueState<BigDecimal> lastValue;
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
                    public void processElement(TagKafkaInfo tagKafkaInfo, KeyedProcessFunction<Tuple3<String,String, Integer>, TagKafkaInfo, TagKafkaInfo>.Context context, Collector<TagKafkaInfo> collector) throws Exception {
                        if (lastValue.value() != null) {
                            BigDecimal jumpValue = tagKafkaInfo.getValue().subtract(lastValue.value());
                            if (jumpValue.compareTo(lower) == -1 || jumpValue.compareTo(upper) == 1) {
                                tagKafkaInfo.setValue(lastValue.value());
                            } else {
                                lastValue.update(tagKafkaInfo.getValue());
                            }
                        } else {
                            lastValue.update(tagKafkaInfo.getValue());
                        }

                        collector.collect(tagKafkaInfo);
                    }
                })
                .setParallelism(ConfigManager.getInteger(PropertiesConstants.STREAM_CALCULATE_PARALLELISM))
                .name("DEJUMP-跳变抑制-range("+lower+","+upper+")");
    }


    @Override
    public void doTransForm(TStream tStream, TransformChain chain) {
        tStream.stream = deJump(tStream.stream, new BigDecimal(lower), new BigDecimal(upper));
        chain.doTransform(tStream);
    }
}
