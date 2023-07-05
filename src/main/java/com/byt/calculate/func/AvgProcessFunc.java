package com.byt.calculate.func;

import com.byt.calculate.IfCalculate;
import com.byt.pojo.TagKafkaInfo;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple7;
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
public class AvgProcessFunc extends ProcessWindowFunction<TagKafkaInfo, TagKafkaInfo, String, TimeWindow> {
    private transient SimpleDateFormat sdf;
    private  OutputTag<TagKafkaInfo> dwdOutPutTag;
    private  ValueState<BigDecimal> sumState;
    private ValueState<BigDecimal> numState;

    public AvgProcessFunc(OutputTag<TagKafkaInfo> dwdOutPutTag) {
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
        numState = getRuntimeContext().getState(
                new ValueStateDescriptor<BigDecimal>(
                        "num-state",
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
            if (numState.value() == null) {
                numState.update(new BigDecimal(1L));
            } else {
                numState.update(numState.value().add(new BigDecimal(1L)));
            }
        }
        //System.out.println(sumState.value() + "--------------" + numState.value());
        BigDecimal avg = sumState.value().divide(numState.value(), 4, BigDecimal.ROUND_HALF_UP);
        TagKafkaInfo result = elements.iterator().next();
        result.setValue(avg);
        result.setTime(sdf.format(context.window().getEnd()));
        result.setTimestamp(null);
        result.setCurrIndex(result.getCurrIndex() + 1);
        if (result.getCurrIndex() < result.getTotalIndex()) {
            // 还需要进行后续运算
            String[] split = result.getCalculateType().split("_");
            result.setCurrCal(split[result.getCurrIndex()]);
            context.output(dwdOutPutTag, result);
        } else if (result.getCurrIndex() == result.getTotalIndex()) {
            // 计算完成直接输出
            result.setCurrCal("over");
            out.collect(result);
        }
        sumState.clear();
        numState.clear();
    }
}
