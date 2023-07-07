package com.byt.tagcalculate.calculate.func;

import com.byt.tagcalculate.pojo.TagKafkaInfo;
import com.byt.common.utils.BytTagUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;

/**
 * @title: RANGE算子函数
 * @author: zhangyf
 * @date: 2023/7/7 14:27
 **/
public class RangeProcessFunc extends ProcessWindowFunction<TagKafkaInfo, TagKafkaInfo, String, TimeWindow> {
    private transient SimpleDateFormat sdf;
    private OutputTag<TagKafkaInfo> dwdOutPutTag;

    public RangeProcessFunc(OutputTag<TagKafkaInfo> dwdOutPutTag) {
        this.dwdOutPutTag = dwdOutPutTag;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    }

    @Override
    public void process(String key, ProcessWindowFunction<TagKafkaInfo, TagKafkaInfo, String, TimeWindow>.Context context, Iterable<TagKafkaInfo> elements, Collector<TagKafkaInfo> out) throws Exception {
        Iterator<TagKafkaInfo> iterator = elements.iterator();
        ArrayList<BigDecimal> arrayList = new ArrayList<>();
        while (iterator.hasNext()) {
            TagKafkaInfo next = iterator.next();
            arrayList.add(next.getValue());
        }
        arrayList.sort(new Comparator<BigDecimal>() {
            @Override
            public int compare(BigDecimal o1, BigDecimal o2) {
                return o1.intValue() - o2.intValue();
            }
        });
        BigDecimal value = arrayList.get(arrayList.size() - 1).subtract(arrayList.get(0));
        TagKafkaInfo tagKafkaInfo = elements.iterator().next();
        tagKafkaInfo.setValue(value);
        tagKafkaInfo.setTime(sdf.format(context.window().getEnd()));
        BytTagUtil.outputByWindow(tagKafkaInfo, context, out, dwdOutPutTag);
        arrayList.clear();
    }
}
