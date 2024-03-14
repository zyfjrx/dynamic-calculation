package com.byt.tagcalculate.sink;

import com.byt.common.utils.EnvironmentUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @title:
 * @author: zhangyf
 * @date: 2024/3/13 16:20
 **/
public class Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        ParameterTool parameterTool = EnvironmentUtils.createParameterTool();
        env.getConfig().setGlobalJobParameters(parameterTool);

        env
                .addSource(new IotDBSource())
                .print();


        env.execute();
    }
}
