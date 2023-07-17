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
 * @title: MEDIAN算子函数
 * @author: zhangyf
 * @date: 2023/7/5 14:38
 **/
public class MedianProcessFunc extends ProcessWindowFunction<TagKafkaInfo, TagKafkaInfo, String, TimeWindow> {
    private  SimpleDateFormat sdf;
    private OutputTag<TagKafkaInfo> dwdOutPutTag;

    public MedianProcessFunc(OutputTag<TagKafkaInfo> dwdOutPutTag) {
        this.dwdOutPutTag = dwdOutPutTag;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    }

    @Override
    public void process(String key, ProcessWindowFunction<TagKafkaInfo, TagKafkaInfo, String, TimeWindow>.Context context, Iterable<TagKafkaInfo> elements, Collector<TagKafkaInfo> out) throws Exception {
        Iterator<TagKafkaInfo> iterator = elements.iterator();
        ArrayList<TagKafkaInfo> arrayList = new ArrayList<>();
        while (iterator.hasNext()) {
            TagKafkaInfo next = iterator.next();
            arrayList.add(next);
        }
        arrayList.sort(new Comparator<TagKafkaInfo>() {
            @Override
            public int compare(TagKafkaInfo o1, TagKafkaInfo o2) {
                return o1.getValue().intValue() - o2.getValue().intValue();
            }
        });
        BigDecimal median = new BigDecimal(0L);
        if (arrayList.size() % 2 == 0) {
            median = arrayList.get(arrayList.size() / 2 - 1).getValue()
                    .add(arrayList.get(arrayList.size() / 2).getValue())
                    .divide(BigDecimal.valueOf(2L), 4, BigDecimal.ROUND_HALF_UP);
        } else {
            median = arrayList.get((arrayList.size() + 1) / 2 - 1).getValue();
        }
        TagKafkaInfo tagKafkaInfo = arrayList.get(arrayList.size() - 1);
        tagKafkaInfo.setTime(sdf.format(context.window().getEnd()));
        tagKafkaInfo.setValue(median);
        BytTagUtil.outputByWindow(tagKafkaInfo,context,out,dwdOutPutTag);
        arrayList.clear();
    }
}
