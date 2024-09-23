package test;

import com.byt.common.utils.EnvironmentUtils;
import com.byt.tagcalculate.sink.MqttSink;
import com.byt.tagcalculate.source.MqttSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @title:
 * @author: zhangyf
 * @date: 2024/7/31 9:39
 **/
public class MqttTest1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        ParameterTool parameterTool = EnvironmentUtils.createParameterTool();
        env.getConfig().setGlobalJobParameters(parameterTool);

        env
                .addSource(new MqttSource())
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String s) throws Exception {
                        return s;
                    }
                })
                .addSink(new MqttSink());


        env.execute();
    }
}
