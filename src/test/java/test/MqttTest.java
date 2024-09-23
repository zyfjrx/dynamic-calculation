package test;

import com.byt.common.utils.EnvironmentUtils;
import com.byt.tagcalculate.source.MqttSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @title:
 * @author: zhangyf
 * @date: 2024/7/31 9:39
 **/
public class MqttTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        ParameterTool parameterTool = EnvironmentUtils.createParameterTool();
        env.getConfig().setGlobalJobParameters(parameterTool);

        env
                .addSource(new MqttSource())
                .print();


        env.execute();
    }
}
