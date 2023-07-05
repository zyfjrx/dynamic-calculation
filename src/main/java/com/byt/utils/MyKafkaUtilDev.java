package com.byt.utils;

import com.byt.constants.PropertiesConstants;
import com.byt.deserialization.ProtoKafkaDeserialization;
import com.byt.deserialization.TagInfoDeserializationSchema;
import com.byt.deserialization.TopicDataDeserialization;
import com.byt.pojo.TagKafkaInfo;
import com.byt.pojo.TopicData;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * @title: kafka工具类
 * @author: zhang
 * @date: 2022/8/17 14:27
 */
public class MyKafkaUtilDev {


    private static String defaultTopic = "DWD_DEFAULT_TOPIC";
    private static Properties properties = new Properties();

    static {
        properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, ConfigManager.getProperty(PropertiesConstants.KAFKA_SERVER));

    }

    /**
     * kafka-消费者 处理时间
     *
     * @param topic
     * @param groupId
     * @return
     */
    public static FlinkKafkaConsumer<List<TagKafkaInfo>> getKafkaListConsumer(String topic, String groupId) {
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return new FlinkKafkaConsumer<List<TagKafkaInfo>>(
                topic,
                new ProtoKafkaDeserialization(),
                properties
        );
    }


    /**
     * kafka-消费者 事件时间
     *
     * @param topic
     * @param groupId
     * @return
     */
    public static FlinkKafkaConsumer<List<TagKafkaInfo>> getKafkaListConsumerWM(List<String> topic, String groupId) {
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        FlinkKafkaConsumer<List<TagKafkaInfo>> kafkaConsumer = new FlinkKafkaConsumer<List<TagKafkaInfo>>(
                topic,
                new ProtoKafkaDeserialization(),
                properties
        );
        // 分配水位线和提取时间戳
//        kafkaConsumer
//               // .setStartFromTimestamp(TimeUtil.getStartTime(startTime))
//                .assignTimestampsAndWatermarks(
//                        WatermarkStrategy.<List<TagKafkaInfo>>forBoundedOutOfOrderness(Duration.ofSeconds(1L))
//                                .withIdleness(Duration.ofSeconds(10L))
//                                .withTimestampAssigner(new SerializableTimestampAssigner<List<TagKafkaInfo>>() {
//                                    @Override
//                                    public long extractTimestamp(List<TagKafkaInfo> list, long l) {
//                                        if (list.size() > 0) {
//                                            return list.get(0).getTimestamp();
//                                        } else {
//                                            return 1606710000000L;
//                                        }
//                                    }
//                                })
//                );
        return kafkaConsumer;
    }


    /**
     * 消费kafka数据转化为pojo类 no Watermark
     *
     * @param topic
     * @param groupId
     * @return
     */
    public static FlinkKafkaConsumer<TagKafkaInfo> getKafkaPojoConsumer(String topic, String groupId) {
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return new FlinkKafkaConsumer<TagKafkaInfo>(
                topic,
                new TagInfoDeserializationSchema(),
                properties
        );
    }

    /**
     * 消费kafka数据转化为pojo类 Watermark
     *
     * @param topic
     * @param groupId
     * @return
     */
    public static FlinkKafkaConsumer<TagKafkaInfo> getKafkaPojoConsumerWM(String topic, String groupId) {
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
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
        return new FlinkKafkaConsumer<String>(
                topic,
                new SimpleStringSchema(),
                properties
        );
    }

    public static FlinkKafkaProducer<String> getKafkaProducer(String topic) {
        return new FlinkKafkaProducer<String>(
                topic,
                new SimpleStringSchema(),
                properties
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
                defaultTopic,
                new TopicDataDeserialization(),
                properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
    }


    //获取FlinkKafkaProducer对象
    public static <T> FlinkKafkaProducer<T> getKafkaSinkBySchema(KafkaSerializationSchema<T> kafkaSerializationSchema) {
        return new FlinkKafkaProducer<T>(
                defaultTopic,
                kafkaSerializationSchema,
                properties,
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
        );
    }


    /**
     * kafka-source DDL语句
     *
     * @param topic   数据源主题
     * @param groupId 消费者组
     * @return
     */
    public static String getKafkaDDL(String topic, String groupId) {
        return "WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = '" + topic + "'," +
                "  'properties.bootstrap.servers' = '" + ConfigManager.getProperty(PropertiesConstants.KAFKA_SERVER) + "'," +
                "  'properties.group.id' = '" + groupId + "'," +
                "  'scan.startup.mode' = 'latest-offset'," +
                "  'format' = 'json'" +
                ")";
    }
    //latest-offset   earliest-offset


    /**
     * Kafka-Sink DDL 语句
     *
     * @param topic 输出到 Kafka 的目标主题
     * @return 拼接好的 Kafka-Sink DDL 语句
     */
    public static String getKafkaSinkDDL(String topic) {
        return " WITH ( " +
                "  'connector' = 'kafka', " +
                "  'topic' = '" + topic + "', " +
                "  'properties.bootstrap.servers' = '" + PropertiesConstants.KAFKA_SERVER + "', " +
                "  'format' = 'json' " +
                ")";
    }

    /**
     * Upsertkafka-Sink DDL 语句
     *
     * @param topic 输出到 Kafka 的目标主题
     * @return 拼接好的 Kafka-Sink DDL 语句
     */
    public static String getUpsertKafkaDDL(String topic) {

        return "WITH ( " +
                "  'connector' = 'upsert-kafka', " +
                "  'topic' = '" + topic + "', " +
                "  'properties.bootstrap.servers' = '" + PropertiesConstants.KAFKA_SERVER + "', " +
                "  'key.format' = 'json', " +
                "  'value.format' = 'json' " +
                ")";
    }

}
