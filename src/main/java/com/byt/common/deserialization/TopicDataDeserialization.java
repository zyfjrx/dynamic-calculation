package com.byt.common.deserialization;

import com.byt.tagcalculate.pojo.TopicData;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * @title:
 * @author: zhangyifan
 * @date: 2022/8/15 13:35
 */
public class TopicDataDeserialization implements KafkaSerializationSchema<TopicData> {
    public TopicDataDeserialization() {
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(TopicData topicData, @Nullable Long aLong) {
        return new ProducerRecord<byte[], byte[]>(
                topicData.getTopic(),
                topicData.getData()
        );
    }
}
