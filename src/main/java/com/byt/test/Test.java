package com.byt.test;

import com.byt.constants.PropertiesConstants;
import com.byt.pojo.TagKafkaInfo;
import com.byt.utils.ConfigManager;
import com.byt.utils.MyKafkaUtilDev;
import lombok.EqualsAndHashCode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.*;

/**
 * @title:
 * @author: zhangyifan
 * @date: 2022/10/11 14:12
 */
@EqualsAndHashCode
public class Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(
                        MyKafkaUtilDev
                                //.getKafkaListConsumerWM(ConfigManager.getListProperty(PropertiesConstants.KAFKA_ODS_TOPIC),
                                .getKafkaListConsumerWM(ConfigManager.getListProperty("kafka.ods.topic"),
                                        ConfigManager.getProperty("kafka.group.id")
                                )).print();

env.execute();
    }
}
