package com.byt.tagcalculate.calculate.calculatechain;

import com.byt.tagcalculate.pojo.TagKafkaInfo;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * @title:
 * @author: zhangyifan
 * @date: 2022/8/29 21:42
 */
public class TStream {
    public DataStream<TagKafkaInfo> stream;

    public TStream(DataStream<TagKafkaInfo> stream) {
        this.stream = stream;
    }
}
