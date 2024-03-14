package com.byt.tagcalculate.sink;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.pool.SessionDataSetWrapper;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.read.common.RowRecord;

/**
 * @title:
 * @author: zhangyf
 * @date: 2024/3/13 16:17
 **/
public class IotDBSource extends RichSourceFunction<String> {

    boolean isRunning = true;

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
    public void run(SourceContext<String> sourceContext) throws Exception {
        while (isRunning){
            SessionDataSetWrapper sessionDataSetWrapper = pool.executeQueryStatement("select * from root.hk_line1.job1");
            SessionDataSet sessionDataSet = sessionDataSetWrapper.getSessionDataSet();
            while (sessionDataSet.hasNext()) {
                RowRecord record = sessionDataSet.next();
                sourceContext.collect(record.toString());
            }
            Thread.sleep(500L);
        }
    }

    @Override
    public void cancel() {

        isRunning = false;
    }
}
