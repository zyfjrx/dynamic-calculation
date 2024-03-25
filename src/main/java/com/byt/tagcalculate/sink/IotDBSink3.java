package com.byt.tagcalculate.sink;


import com.byt.tagcalculate.pojo.OdsTagRecord;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.iotdb.session.pool.SessionPool;

/**
 * @title:
 * @author: zhangyf
 * @date: 2024/3/17 16:26
 **/
public class IotDBSink3 extends RichSinkFunction<OdsTagRecord> {
    private SessionPool pool;

    private ParameterTool parameterTool;
    private String device;


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
        device = "root." + parameterTool.get("iotdb.devicePrefix") + ".line"+parameterTool.get("iotdb.lineId")+"."+parameterTool.get("iotdb.dataLevel");

    }


    @Override
    public void invoke(OdsTagRecord odsTagRecord, Context context) throws Exception {
        try {
            long start = System.currentTimeMillis();
            pool.insertRecordsOfOneDevice(
                    device,
                    odsTagRecord.getTimestamps(),
                    odsTagRecord.getTagNames(),
                    odsTagRecord.getTypes(),
                    odsTagRecord.getValues()
            );
            long end = System.currentTimeMillis();
            System.out.println(String.format("duration[%s]ms", (end - start)) + "----> insert:" + odsTagRecord.getTagNames().size());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws Exception {
        pool.close();
    }
}

