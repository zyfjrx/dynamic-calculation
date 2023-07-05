package com.byt.calculate.window;

import com.byt.calculate.TStream;
import com.byt.calculate.Transform;
import com.byt.calculate.TransformChain;
import com.byt.pojo.TagKafkaInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.List;

/**
 * @title: 获取原始数据算子
 * @author: zhangyifan
 * @date: 2022/9/8 10:48
 */
public class RAW implements Transform {


    public RAW(List<String> params) {
    }

    public DataStream<TagKafkaInfo> raw(DataStream<TagKafkaInfo> in) {
        return in.keyBy(new KeySelector<TagKafkaInfo, Tuple3<String,String, Integer>>() {
            @Override
            public Tuple3<String,String, Integer> getKey(TagKafkaInfo tagKafkaInfo) throws Exception {
                return Tuple3.of(
                        tagKafkaInfo.getTopic(),
                        tagKafkaInfo.getBytName(),
                        tagKafkaInfo.getLineId()
                );
            }
        });
    }

    @Override
    public void doTransForm(TStream tStream, TransformChain chain) {
        tStream.stream = raw(tStream.stream);
        chain.doTransform(tStream);
    }
}
