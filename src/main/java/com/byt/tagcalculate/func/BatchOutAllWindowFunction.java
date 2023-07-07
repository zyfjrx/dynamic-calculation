package com.byt.tagcalculate.func;


import com.byt.tagcalculate.pojo.TagKafkaInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @title:
 * @author: zhangyifan
 * @date: 2022/7/6 13:49
 */
public class BatchOutAllWindowFunction extends ProcessAllWindowFunction<TagKafkaInfo, List<TagKafkaInfo>, TimeWindow> {
    private ArrayList<TagKafkaInfo> tags ;
    @Override
    public void open(Configuration parameters) throws Exception {
        tags = new ArrayList<>();
    }

    @Override
    public void process(ProcessAllWindowFunction<TagKafkaInfo, List<TagKafkaInfo>, TimeWindow>.Context context, Iterable<TagKafkaInfo> elements, Collector<List<TagKafkaInfo>> out) throws Exception {
        ArrayList<TagKafkaInfo> tags = new ArrayList<>();
        Iterator<TagKafkaInfo> iterator = elements.iterator();
        while (iterator.hasNext()) {
            TagKafkaInfo tagKafkaInfo = iterator.next();
            tags.add(tagKafkaInfo);
        }
        if (tags.size() > 0) {
            out.collect(tags);
            tags.clear();
        }
    }
}
