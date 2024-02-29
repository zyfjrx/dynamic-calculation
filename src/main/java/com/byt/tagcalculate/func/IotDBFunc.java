package com.byt.tagcalculate.func;

import com.byt.tagcalculate.pojo.TagKafkaInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IotDBFunc extends ProcessFunction<List<TagKafkaInfo>, Map<String,String>> {
    private String separator;
    private Map<String, String> tuple1 = new HashMap<>();

    public IotDBFunc(String separator) {
        this.separator = separator;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        tuple1.put("device", "root.hk.line2");
        tuple1.put("measurements", "byt_name"+separator+"tag_topic"+separator+"value"+separator+"calculate_time"+separator+"calculate_type"+separator+"calculate_params"+separator+"job_name"+separator+"line_id");
        tuple1.put("types", "TEXT"+separator+"TEXT"+separator+"DOUBLE"+separator+"TEXT"+separator+"TEXT"+separator+"TEXT"+separator+"TEXT"+separator+"INT32");
    }

    @Override
    public void processElement(List<TagKafkaInfo> tagKafkaInfos, ProcessFunction<List<TagKafkaInfo>, Map<String, String>>.Context context, Collector<Map<String, String>> collector) throws Exception {
        for (TagKafkaInfo tagKafkaInfo : tagKafkaInfos) {
            tuple1.put("values",
                    tagKafkaInfo.getBytName()+separator+
                    tagKafkaInfo.getTopic()+separator+
                            tagKafkaInfo.getValue()+separator+
                            tagKafkaInfo.getTime()+separator+
                            tagKafkaInfo.getCalculateType()+separator+
                            tagKafkaInfo.getCalculateParam()+separator+
                            tagKafkaInfo.getTaskName()+separator+
                            tagKafkaInfo.getLineId()
            );
            collector.collect(tuple1);
        }
    }
}
