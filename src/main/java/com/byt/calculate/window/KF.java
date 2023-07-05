package com.byt.calculate.window;

import com.byt.calculate.TStream;
import com.byt.calculate.Transform;
import com.byt.calculate.TransformChain;
import com.byt.constants.PropertiesConstants;
import com.byt.func.KalmanFilterMapFunction;
import com.byt.pojo.TagKafkaInfo;
import com.byt.utils.ConfigManager;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.List;

/**
 * @title: 卡尔曼滤波器 kalman filter
 * @author: zhangyifan
 * @date: 2022/8/30 13:26
 */
public class KF implements Transform {
    private Double dt;
    private Double r;

    public KF(List<String> params) {
        this.dt = Double.parseDouble(params.get(0));
        this.r = Double.parseDouble(params.get(1));
    }
    public DataStream<TagKafkaInfo> kalmanFilter(DataStream<TagKafkaInfo> in, Double dt,Double r) {
        return in
                .keyBy(new KeySelector<TagKafkaInfo, Tuple3<String,String, Integer>>() {
                    @Override
                    public Tuple3<String,String, Integer> getKey(TagKafkaInfo tagKafkaInfo) throws Exception {
                        return Tuple3.of(
                                tagKafkaInfo.getTopic(),
                                tagKafkaInfo.getBytName(),
                                tagKafkaInfo.getLineId()
                        );
                    }
                })
                .flatMap(new KalmanFilterMapFunction(dt,r))
                .setParallelism(1)
                .name("KF-卡尔曼滤波-("+dt+","+r+")");
    }

    @Override
    public void doTransForm(TStream tStream, TransformChain chain) {
        tStream.stream = kalmanFilter(tStream.stream,dt,r);
        chain.doTransform(tStream);
    }
}
