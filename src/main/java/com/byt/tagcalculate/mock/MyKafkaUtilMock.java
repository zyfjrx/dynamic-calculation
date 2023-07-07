package com.byt.tagcalculate.mock;

import com.byt.tagcalculate.constants.PropertiesConstants;
import com.byt.common.deserialization.TopicDataDeserialization;
import com.byt.tagcalculate.pojo.TopicData;
import com.byt.common.utils.ConfigManager;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * @title: kafka工具类
 * @author: zhang
 * @date: 2022/8/17 14:27
 */
public class MyKafkaUtilMock {


    private static String defaultTopic = "DWD_DEFAULT_TOPIC";
    private static Properties properties = new Properties();

    static {
        properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, ConfigManager.getProperty(PropertiesConstants.KAFKA_SERVER));

    }

    /**
     * 回流ods生产者
     *
     * @return
     */
    public static FlinkKafkaProducer getProducerWithTopicData() {
        properties.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 15 * 60 * 1000L + "");
        return new FlinkKafkaProducer<TopicData>(
                defaultTopic,
                new TopicDataDeserialization(),
                properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
    }



}
