package com.byt.tagcalculate.source;

import com.byt.common.utils.EnvironmentUtils;
import com.byt.tagcalculate.iotdb.source.IoTDBSource;
import com.byt.tagcalculate.iotdb.source.IoTDBSourceOptions;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iotdb.tsfile.read.common.RowRecord;

/**
 * @title:
 * @author: zhangyf
 * @date: 2024/3/20 13:35
 **/
public class Test {
    public static void main(String[] args) throws Exception {
        // 获取执行环境和相关参数
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //System.setProperty("HADOOP_USER_NAME", "root");
        ParameterTool parameterTool = EnvironmentUtils.createParameterTool();
        env.getConfig().setGlobalJobParameters(parameterTool);
        IoTDBSourceOptions ioTDBSourceOptions = new IoTDBSourceOptions(
                parameterTool.get("iotdb.host"),
                parameterTool.getInt("iotdb.port"),
                parameterTool.get("iotdb.username"),
                parameterTool.get("iotdb.password"),
                "select * from root.hf.line1.abc",
                1000
        );
        env
                .addSource(new IoTDBSource<String>(ioTDBSourceOptions) {
                    @Override
                    public String convert(RowRecord var1) {

                        return var1.toString();
                    }
                })
                .print();


        env.execute();

    }
}
