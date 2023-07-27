package com.byt.tagcalculate.calculate.func;

import com.byt.common.utils.BytTagUtil;
import com.byt.tagcalculate.pojo.TagKafkaInfo;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Iterator;

/**
 * @title: AVG算子函数
 * @author: zhangyf
 * @date: 2023/7/5 14:27
 **/
public class AvgProcessFuncPlus extends ProcessWindowFunction<TagKafkaInfo, TagKafkaInfo, String, TimeWindow> {
    private SimpleDateFormat sdf;
    private OutputTag<TagKafkaInfo> dwdOutPutTag;
    private AggregatingState<TagKafkaInfo, TagKafkaInfo> aggregatingState;


    public AvgProcessFuncPlus(OutputTag<TagKafkaInfo> dwdOutPutTag) {
        this.dwdOutPutTag = dwdOutPutTag;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        aggregatingState = getRuntimeContext()
                .getAggregatingState(
                        new AggregatingStateDescriptor<TagKafkaInfo, Tuple3<TagKafkaInfo, BigDecimal, BigDecimal>, TagKafkaInfo>(
                                "aggregatingState",
                                new AggregateFunction<TagKafkaInfo, Tuple3<TagKafkaInfo, BigDecimal, BigDecimal>, TagKafkaInfo>() {
                                    @Override
                                    public Tuple3<TagKafkaInfo, BigDecimal, BigDecimal> createAccumulator() {
                                        return Tuple3.of(new TagKafkaInfo(), BigDecimal.ZERO, BigDecimal.ZERO);
                                    }

                                    @Override
                                    public Tuple3<TagKafkaInfo, BigDecimal, BigDecimal> add(TagKafkaInfo tagKafkaInfo, Tuple3<TagKafkaInfo, BigDecimal, BigDecimal> accumulator) {
                                        accumulator.f0 = tagKafkaInfo;
                                        accumulator.f1 = accumulator.f1.add(tagKafkaInfo.getValue());
                                        accumulator.f2 = accumulator.f2.add(BigDecimal.ONE);
                                        return accumulator;
                                    }

                                    @Override
                                    public TagKafkaInfo getResult(Tuple3<TagKafkaInfo, BigDecimal, BigDecimal> accumulator) {
                                        BigDecimal value = null;
                                        try {
                                            value = accumulator.f1.divide(accumulator.f2, 4, BigDecimal.ROUND_HALF_UP);
                                        } catch (Exception e) {
                                            e.printStackTrace();
                                            System.out.println("AVG计算异常");
                                        }
                                        accumulator.f0.setValue(value);
                                        return accumulator.f0;
                                    }

                                    @Override
                                    public Tuple3<TagKafkaInfo, BigDecimal, BigDecimal> merge(Tuple3<TagKafkaInfo, BigDecimal, BigDecimal> acc1, Tuple3<TagKafkaInfo, BigDecimal, BigDecimal> acc2) {
                                        return Tuple3.of(
                                                acc1.f0,
                                                acc1.f1.add(acc2.f1),
                                                acc1.f2.add(acc2.f2)
                                        );
                                    }
                                },
                                Types.TUPLE(Types.POJO(TagKafkaInfo.class), Types.BIG_DEC, Types.BIG_DEC)
                        )
                );
    }

    @Override
    public void process(String key, ProcessWindowFunction<TagKafkaInfo, TagKafkaInfo, String, TimeWindow>.Context context, Iterable<TagKafkaInfo> elements, Collector<TagKafkaInfo> out) throws Exception {
        Iterator<TagKafkaInfo> iterator = elements.iterator();
        while (iterator.hasNext()) {
            TagKafkaInfo tagKafkaInfo = iterator.next();
            aggregatingState.add(tagKafkaInfo);
        }
        TagKafkaInfo tagKafkaInfo = aggregatingState.get();
        tagKafkaInfo.setTime(sdf.format(context.window().getEnd()));
        BytTagUtil.outputByWindow(tagKafkaInfo, context, out, dwdOutPutTag);
        aggregatingState.clear();
    }
}
