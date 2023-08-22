package com.byt.common.utils;

import com.byt.common.deserialization.ProtoKafkaDeserialization;
import com.byt.common.deserialization.TagInfoDeserializationSchema;
import com.byt.common.deserialization.TopicDataDeserialization;
import com.byt.tagcalculate.pojo.TagKafkaInfo;
import com.byt.tagcalculate.pojo.TopicData;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;

/**
 * @title: kafka工具类
 * @author: zhang
 * @date: 2022/8/17 14:27
 */
public class MyKafkaUtil {


    private static String defaultTopic = "DWD_DEFAULT_TOPIC";
    private static Properties properties = new Properties();


//    static {
//        properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, ConfigManager.getProperty(PropertiesConstants.KAFKA_SERVER));
//        //会开启一个后台线程每隔5s检测一下Kafka的分区情况,实现动态分区检测
////        properties.setProperty("flink.partition-discovery.interval-millis", "5000");
//
//    }


    /**
     * kafka-消费者 事件时间
     *
     * @param topic
     * @param groupId
     * @return
     */
    public static FlinkKafkaConsumer<List<TagKafkaInfo>> getKafkaListConsumer(List<String> topic, String groupId,String server) {
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
//        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
//        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "6000");
        properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,server);
        FlinkKafkaConsumer<List<TagKafkaInfo>> kafkaConsumer = new FlinkKafkaConsumer<List<TagKafkaInfo>>(
                topic,
                new ProtoKafkaDeserialization(),
                properties
        );
        return kafkaConsumer;
    }


    /**
     * 消费kafka数据转化为pojo类 Watermark
     *
     * @param topic
     * @param groupId
     * @return
     */
    public static FlinkKafkaConsumer<TagKafkaInfo> getKafkaPojoConsumer(String topic, String groupId,String server) {
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,server);
        FlinkKafkaConsumer<TagKafkaInfo> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic,
                new TagInfoDeserializationSchema(),
                properties
        );
        return kafkaConsumer;
    }


    /**
     * 消费kafka数据转化为pojo类 Watermark
     *
     * @param topics
     * @param groupId
     * @return
     */
    public static FlinkKafkaConsumer<TagKafkaInfo> getKafkaPojoConsumerWithTopics(List<String> topics, String groupId) {
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        FlinkKafkaConsumer<TagKafkaInfo> kafkaConsumer = new FlinkKafkaConsumer<>(
                topics,
                new TagInfoDeserializationSchema(),
                properties
        );
        return kafkaConsumer;
    }

    /**
     * kafka-消费者
     *
     * @param topic
     * @param groupId
     * @return
     */
    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic, String groupId) {
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new FlinkKafkaConsumer<String>(
                topic,
                new SimpleStringSchema(),
                properties
        );
    }

    public static FlinkKafkaProducer<String> getKafkaProducer(String topic,String server) {
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,server);
        properties.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 15 * 60 * 1000L + "");
        return new FlinkKafkaProducer<String>(
                "DEFAULT_TOPIC",
                new KafkaSerializationSchema<String>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
                        return new ProducerRecord<byte[], byte[]>(topic,element.getBytes(StandardCharsets.UTF_8));
                    }
                },
                properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
    }


    /**
     * 回流ods生产者
     *
     * @return
     */
    public static FlinkKafkaProducer getProducerWithTopicData() {
        properties.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 15 * 60 * 1000L + "");
        return new FlinkKafkaProducer<TopicData>(
                "DEFAULT_TOPIC",
                new TopicDataDeserialization(),
                properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
    }


    //获取FlinkKafkaProducer对象
    public static <T> FlinkKafkaProducer<T> getKafkaSinkBySchema(KafkaSerializationSchema<T> kafkaSerializationSchema) {
        return new FlinkKafkaProducer<T>(
                "DEFAULT_TOPIC",
                kafkaSerializationSchema,
                properties,
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
        );
    }

}
