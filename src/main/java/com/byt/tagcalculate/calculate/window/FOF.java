package com.byt.tagcalculate.calculate.window;

import com.byt.tagcalculate.calculate.TStream;
import com.byt.tagcalculate.calculate.Transform;
import com.byt.tagcalculate.calculate.TransformChain;
import com.byt.tagcalculate.constants.PropertiesConstants;
import com.byt.tagcalculate.func.FirstOrderFilterFunction;
import com.byt.tagcalculate.pojo.TagKafkaInfo;
import com.byt.common.utils.ConfigManager;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.math.BigDecimal;
import java.util.List;

/**
 * @title: 一阶低通滤波算子
 *  Y(n)=αX(n) + (1-α)Y(n-1)
 * @author: zhangyifan
 * @date: 2022/7/28 13:39
 */
public class FOF implements Transform {
    // 变异系数
    private double a;

    public FOF(List<String> params) {
        this.a = Double.parseDouble(params.get(0));
    }

    public DataStream<TagKafkaInfo> firstOrder(DataStream<TagKafkaInfo> in, BigDecimal a) {
        return in
                .keyBy(new KeySelector<TagKafkaInfo, Tuple3<String,String, Integer>>() {
                    @Override
                    public Tuple3<String, String,Integer> getKey(TagKafkaInfo tagKafkaInfo) throws Exception {
                        return Tuple3.of(
                                tagKafkaInfo.getTopic(),
                                tagKafkaInfo.getBytName(),
                                tagKafkaInfo.getLineId()
                        );
                    }
                })
                .process(new FirstOrderFilterFunction(a))
                .setParallelism(ConfigManager.getInteger(PropertiesConstants.STREAM_CALCULATE_PARALLELISM))
                .name("FOF-一阶低通滤波-系数："+a);
    }


    @Override
    public void doTransForm(TStream tStream, TransformChain chain) {
        tStream.stream = firstOrder(tStream.stream, new BigDecimal(a));
        chain.doTransform(tStream);
    }
}
