package com.byt.tagcalculate.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Arrays;
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TopicData {
    private String topic;
    private byte[] data;
    private byte[] key;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public byte[] getKey() {
        return key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    @Override
    public String toString() {
        return "TopicData{" +
                "topic='" + topic + '\'' +
                ", data=" + Arrays.toString(data) +
                ", key=" + Arrays.toString(key) +
                '}';
    }
}
