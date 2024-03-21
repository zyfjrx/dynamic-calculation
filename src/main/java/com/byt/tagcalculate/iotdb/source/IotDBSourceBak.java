package com.byt.tagcalculate.iotdb.source;


import com.byt.tagcalculate.sink.IotDBSession;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.pool.SessionDataSetWrapper;
import org.apache.iotdb.session.pool.SessionPool;

/**
 * @title:
 * @author: zhangyf
 * @date: 2024/3/17 16:26
 **/
public class IotDBSourceBak extends RichSourceFunction<String> {
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
    public void close() throws Exception {
        pool.close();
    }

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        SessionDataSetWrapper sessionDataSetWrapper = pool.executeQueryStatement("select * from root.hf.ods.A0209_E3_SC1_2503_1030_QD");
        SessionDataSet sessionDataSet = sessionDataSetWrapper.getSessionDataSet();
        SessionDataSet.DataIterator iterator = sessionDataSet.iterator();
        while (iterator.next()) {
            long time = iterator.getLong("Time");
            String value = iterator.getString("root.hf.ods.A0209_E3_SC1_2503_1030_QD.value");
            sourceContext.collect(time+"-->"+value);
        }

    }

    @Override
    public void cancel() {

    }
}

