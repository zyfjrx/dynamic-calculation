package com.byt.tagcalculate.func;

import com.byt.common.utils.TimeUtil;
import com.byt.tagcalculate.pojo.Event;
import com.byt.tagcalculate.pojo.OdsTag;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class OdsTagListToEventFunc extends ProcessFunction<List<OdsTag>, List<Event>> {
    //private List<String> measurements;
    private List<TSDataType> types;

    //private List<Object> values;
    private List<Event> events;

    private ParameterTool parameterTool;

    private String device;


    @Override
    public void open(Configuration parameters) throws Exception {
        types = Arrays.asList(
                TSDataType.DOUBLE
        );
        events = new ArrayList<>();
        parameterTool = (ParameterTool)
                getRuntimeContext()
                        .getExecutionConfig()
                        .getGlobalJobParameters();

        device = "root." + parameterTool.get("iotdb.devicePrefix") + ".line"+parameterTool.get("iotdb.lineId")+"."+parameterTool.get("iotdb.dataLevel");
    }


    @Override
    public void processElement(List<OdsTag> odsTags, ProcessFunction<List<OdsTag>, List<Event>>.Context context, Collector<List<Event>> collector) throws Exception {
        try {
            for (OdsTag odsTag : odsTags) {
                List<String> measurements = Arrays.asList(odsTag.getTagName());
                List<Object> values = Arrays.asList(odsTag.getValue());
                Event event = new Event(device, TimeUtil.getStartTime(odsTag.getTime()), measurements, types, values);
                events.add(event);
            }
            collector.collect(events);
            events.clear();
        } catch (Exception e) {
            events.clear();
            e.printStackTrace();
            System.out.println("convert to event failed ! Cause:" + e.getMessage() + "------>" + odsTags);
        }
    }
}

