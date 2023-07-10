package com.byt.tagcalculate.calculate.func;

import com.byt.common.utils.BytTagUtil;
import com.byt.tagcalculate.pojo.TagKafkaInfo;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
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
 * @date: 2023/7/10 14:27
 **/
public class SumProcessFunc extends ProcessWindowFunction<TagKafkaInfo, TagKafkaInfo, String, TimeWindow> {
    private transient SimpleDateFormat sdf;
    private  OutputTag<TagKafkaInfo> dwdOutPutTag;
    private  ValueState<BigDecimal> sumState;

    public SumProcessFunc(OutputTag<TagKafkaInfo> dwdOutPutTag) {
        this.dwdOutPutTag = dwdOutPutTag;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        sumState = getRuntimeContext().getState(
                new ValueStateDescriptor<BigDecimal>(
                        "sum-state",
                        Types.BIG_DEC
                )
        );
    }

    @Override
    public void process(String key, ProcessWindowFunction<TagKafkaInfo, TagKafkaInfo, String, TimeWindow>.Context context, Iterable<TagKafkaInfo> elements, Collector<TagKafkaInfo> out) throws Exception {
        Iterator<TagKafkaInfo> iterator = elements.iterator();
        while (iterator.hasNext()) {
            TagKafkaInfo tagKafkaInfo = iterator.next();
            if (sumState.value() == null) {
                sumState.update(tagKafkaInfo.getValue());
            } else {
                sumState.update(sumState.value().add(tagKafkaInfo.getValue()));
            }
        }
        TagKafkaInfo tagKafkaInfo = elements.iterator().next();
        tagKafkaInfo.setValue(sumState.value());
        tagKafkaInfo.setTime(sdf.format(context.window().getEnd()));
        BytTagUtil.outputByWindow(tagKafkaInfo,context,out,dwdOutPutTag);
        sumState.clear();
    }
}
