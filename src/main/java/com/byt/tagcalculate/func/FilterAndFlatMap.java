package com.byt.tagcalculate.func;

import com.alibaba.fastjson.JSONObject;
import com.byt.tagcalculate.pojo.TagKafkaInfo;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * @title:
 * @author: zhangyifan
 * @date: 2022/7/6 10:18
 */
public class FilterAndFlatMap extends RichFlatMapFunction<Tuple2<String, List<TagKafkaInfo>>, String> {

    private String startJobs;

    public FilterAndFlatMap(String startJobs) {
        this.startJobs = startJobs;
    }

    public FilterAndFlatMap() {
    }

    @Override
    public void flatMap(Tuple2<String, List<TagKafkaInfo>> data, Collector<String> collector) throws Exception {
        List<TagKafkaInfo> tagKafkaInfos = data.f1;
        //System.out.println(data);
        if (!(tagKafkaInfos.isEmpty()) && data.f0.equals("normal")) {
            for (TagKafkaInfo tagKafkaInfo : tagKafkaInfos) {
                collector.collect(JSONObject.toJSONString(tagKafkaInfo));
            }
        }
    }
}
