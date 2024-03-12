package com.byt.tagcalculate.sink;


import com.byt.tagcalculate.pojo.Event;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.ArrayList;
import java.util.List;

/**
 * @title:
 * @author: zhangyf
 * @date: 2024/3/17 16:26
 **/
public class IotDBSink extends RichSinkFunction<List<Event>> {
    private SessionPool pool;

    private ParameterTool parameterTool;

    @Override
    public void open(Configuration parameters) throws Exception {
        parameterTool = (ParameterTool)
                getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        pool = IotDBSession.getSession(
                parameterTool.get("iotdb.host"),
                parameterTool.getInt("iotdb.port"),
                parameterTool.get("iotdb.username"),
                parameterTool.get("iotdb.password"),
                parameterTool.getInt("iotdb.poolSize")
        );
    }


    @Override
    public void invoke(List<Event> events, Context context) throws Exception {
        try {
            long start = System.currentTimeMillis();
            List<String> deviceIds = new ArrayList<>();
            List<Long> timestamps = new ArrayList<>();
            List<List<String>> measurementsList = new ArrayList<>();
            List<List<TSDataType>> typesList = new ArrayList<>();
            List<List<Object>> valuesList = new ArrayList<>();
            int cnt = 0;
            for (Event event : events) {
                deviceIds.add(event.getDevice());
                timestamps.add(event.getTimestamp());
                measurementsList.add(event.getMeasurements());
                typesList.add(event.getTypes());
                valuesList.add(event.getValues());
                cnt++;
                if (cnt % 200 == 0) {
                    pool.insertRecords(deviceIds, timestamps, measurementsList, typesList, valuesList);
                    deviceIds.clear();
                    timestamps.clear();
                    measurementsList.clear();
                    typesList.clear();
                    valuesList.clear();
                }
            }
            if (deviceIds.size() > 0) {
                pool.insertRecords(deviceIds, timestamps, measurementsList, typesList, valuesList);
            }
            long end = System.currentTimeMillis();
            System.out.println(String.format("duration[%s]ms", (end - start))+"----> insert:"+cnt);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public void close() throws Exception {
        pool.close();
    }
}

