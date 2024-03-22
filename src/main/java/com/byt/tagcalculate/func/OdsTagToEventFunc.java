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

public class OdsTagToEventFunc extends ProcessFunction<OdsTag, Event> {
    private List<String> measurements;
    private List<TSDataType> types;

    private List<Object> values;

    private ParameterTool parameterTool;

    @Override
    public void open(Configuration parameters) throws Exception {
        measurements = new ArrayList<>(1);
        types = Arrays.asList(
                TSDataType.DOUBLE
                //TSDataType.TEXT

        );
        values = new ArrayList<>(1);
        parameterTool = (ParameterTool)
                getRuntimeContext()
                        .getExecutionConfig()
                        .getGlobalJobParameters();
    }


    @Override
    public void processElement(OdsTag odsTag, ProcessFunction<OdsTag, Event>.Context context, Collector<Event> collector) throws Exception {
        try {
            String device = "root." + parameterTool.get("iotdb.devicePrefix") + ".line"+parameterTool.get("iotdb.lineId")+"."+parameterTool.get("iotdb.dataLevel");
            measurements.add(odsTag.getTagName());
            //measurements.add("create_time");
            values.add(odsTag.getValue());
            //values.add(odsTag.getTime());
            Event event = new Event(device, TimeUtil.getStartTime(odsTag.getTime()), measurements, types, values);
            collector.collect(event);
            values.clear();
            measurements.clear();
        } catch (Exception e) {
            values.clear();
            measurements.clear();
            e.printStackTrace();
            System.out.println("convert to event failed ! Cause:" + e.getMessage() + "------>" + odsTag);
        }finally {
            values.clear();
            measurements.clear();
        }
    }
}

