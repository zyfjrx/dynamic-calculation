package com.byt.calculate.func;


import com.byt.pojo.TagKafkaInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;

/**
 * @title: 最接近计算时刻的数据点取值
 * @author: zhangyf
 * @date: 2023/7/10 14:38
 **/
public class InterpProcessFunc extends ProcessWindowFunction<TagKafkaInfo, TagKafkaInfo, String, TimeWindow> {
    private transient SimpleDateFormat sdf;
    private OutputTag<TagKafkaInfo> dwdOutPutTag;

    public InterpProcessFunc(OutputTag<TagKafkaInfo> dwdOutPutTag) {
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
                return (int) (o1.getTimestamp() - o2.getTimestamp());
            }
        });
        TagKafkaInfo tagKafkaInfo = arrayList.get(arrayList.size() - 1);
        tagKafkaInfo.setTime(sdf.format(context.window().getEnd()));
        tagKafkaInfo.setCurrIndex(tagKafkaInfo.getCurrIndex() + 1);
        if (tagKafkaInfo.getCurrIndex() < tagKafkaInfo.getTotalIndex()){
            tagKafkaInfo.setCurrCal(tagKafkaInfo.getCalculateType().split("_")[tagKafkaInfo.getCurrIndex()]);
            context.output(dwdOutPutTag,tagKafkaInfo);
        } else if (tagKafkaInfo.getCurrIndex() == tagKafkaInfo.getTotalIndex()){
            tagKafkaInfo.setCurrCal("over");
            out.collect(tagKafkaInfo);
        }
        arrayList.clear();
    }
}
