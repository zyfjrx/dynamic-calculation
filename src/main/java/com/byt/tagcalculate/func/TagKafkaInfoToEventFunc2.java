package com.byt.tagcalculate.func;

import com.byt.tagcalculate.pojo.Event;
import com.byt.tagcalculate.pojo.TagKafkaInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TagKafkaInfoToEventFunc2 extends ProcessFunction<TagKafkaInfo, Event> {
    private List<String> measurements;
    private List<TSDataType> types;

    private List<Object> values;

    private ParameterTool parameterTool;

    @Override
    public void open(Configuration parameters) throws Exception {
        measurements = new ArrayList<>(2);
        types = Arrays.asList(
                TSDataType.TEXT,
                TSDataType.TEXT
        );
        values = new ArrayList<>(2);
        parameterTool = (ParameterTool)
                getRuntimeContext()
                        .getExecutionConfig()
                        .getGlobalJobParameters();
    }

    @Override
    public void processElement(TagKafkaInfo tagKafkaInfo, ProcessFunction<TagKafkaInfo, Event>.Context context, Collector<Event> collector) throws Exception {
        try {
            String device = "root." + parameterTool.get("iotdb.devicePrefix") + ".line" + tagKafkaInfo.getLineId() + "." + tagKafkaInfo.getTaskName();
            measurements.add(tagKafkaInfo.getBytName());
            measurements.add("create_time");
            values.add(tagKafkaInfo.getValue().toString());
            values.add(tagKafkaInfo.getTime());
            Event event = new Event(device, tagKafkaInfo.getTimestamp(), measurements, types, values);
            collector.collect(event);
            values.clear();
            measurements.clear();
        } catch (Exception e) {
            values.clear();
            measurements.clear();
            e.printStackTrace();
            System.out.println("convert to event failed ! Cause:" + e.getMessage() + "------>" + tagKafkaInfo);
        }finally {
            values.clear();
            measurements.clear();
        }

    }
}

