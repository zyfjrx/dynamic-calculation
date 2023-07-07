package com.byt.tagcalculate.main;

import com.byt.tagcalculate.constants.PropertiesConstants;
import com.byt.tagcalculate.func.BatchOutAllWindowFunction;
import com.byt.tagcalculate.sink.DbResultBatchSink;
import com.byt.common.utils.ConfigManager;
import com.byt.common.utils.MyKafkaUtilDev;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @title: 同步kafka dws层数据
 * @author: zhangyf
 * @date: 2023/7/7 16:26
 **/
public class Kafka2MySQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                // 2.1 添加数据源
                .addSource(MyKafkaUtilDev.getKafkaPojoConsumerWM(
                        ConfigManager.getProperty("kafka.dwd.topic"),
                        "Kafka2MySQL")
                )
                .windowAll(TumblingProcessingTimeWindows.of(Time.minutes(3)))
                .process(new BatchOutAllWindowFunction())
                .addSink(new DbResultBatchSink(ConfigManager.getProperty(PropertiesConstants.DWS_TODAY_TABLE)))
                .name("Kafka2MySQL");


        env.execute("Kafka2MySQL");
    }
}
