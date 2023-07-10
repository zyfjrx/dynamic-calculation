package com.byt.tagcalculate.calculate.window;

import com.byt.tagcalculate.calculate.calculatechain.TStream;
import com.byt.tagcalculate.calculate.calculatechain.Transform;
import com.byt.tagcalculate.calculate.calculatechain.TransformChain;
import com.byt.tagcalculate.pojo.TagKafkaInfo;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * @title: RSI-相对强弱指标
 * @author: zhangyf
 * @date: 2023/3/21 16:20
 **/
public class RSI implements Transform {
    private Integer n;

    public RSI(List<String> params) {
        this.n = Integer.parseInt(params.get(0));
    }

    public DataStream<TagKafkaInfo> rsi(DataStream<TagKafkaInfo> in,Integer n){
        return in
                .keyBy(new KeySelector<TagKafkaInfo, Tuple7<String, String, String, String, String, Integer, String>>() {
                    @Override
                    public Tuple7<String, String, String, String, String, Integer, String> getKey(TagKafkaInfo r) throws Exception {
                        return Tuple7.of(r.getName(), r.getTopic(), r.getBytName(), r.getCalculateParam(), r.getCalculateType(), r.getLineId(), r.getTaskName());
                    }
                })
                .flatMap(new RichFlatMapFunction<TagKafkaInfo, TagKafkaInfo>() {
                    private transient Queue<Double> gainQueue; // 上涨幅度列表
                    private transient Queue<Double> lossQueue; // 下跌幅度列表
                    private transient ValueState<Double> prevClosePrice; // 前一日收盘价

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        gainQueue = new LinkedList<Double>();
                        lossQueue = new LinkedList<Double>();
                        prevClosePrice = getRuntimeContext().getState(
                                new ValueStateDescriptor<Double>(
                                        "prevClosePrice",
                                        Double.class
                                )
                        );
                    }

                    @Override
                    public void flatMap(TagKafkaInfo tagKafkaInfo, Collector<TagKafkaInfo> collector) throws Exception {
                        if (prevClosePrice.value() == null){
                            gainQueue.offer(0.0);
                            lossQueue.offer(0.0);
                        }
                        if (prevClosePrice.value() != null) {
                            double gain = Math.max(tagKafkaInfo.getValue().doubleValue() - prevClosePrice.value(), 0);
                            double loss = Math.max(prevClosePrice.value() - tagKafkaInfo.getValue().doubleValue(), 0);
                            gainQueue.offer(gain);
                            lossQueue.offer(loss);
                        }
                        prevClosePrice.update(tagKafkaInfo.getValue().doubleValue());

                        if (gainQueue.size() > n-1 && lossQueue.size() > n-1) {
                            double avgGain = gainQueue.stream().mapToDouble(Double::doubleValue).average().getAsDouble();
                            double avgLoss = lossQueue.stream().mapToDouble(Double::doubleValue).average().getAsDouble();
//                            System.out.println("avgGain"+avgGain);
//                            System.out.println("avgLoss"+avgLoss);
//                            System.out.println("queue1: "+gainQueue);
//                            System.out.println("queue2: "+lossQueue);
                            double rs = avgGain / avgLoss;
                            double rsi = 100 - (100 / (1 + rs));
                            gainQueue.poll();
                            lossQueue.poll();
                            tagKafkaInfo.setValue(BigDecimal.valueOf(rsi).setScale(4,BigDecimal.ROUND_HALF_UP));
                            collector.collect(tagKafkaInfo);
                        }

                    }

                });
    }


    @Override
    public void doTransForm(TStream tStream, TransformChain chain) {
        tStream.stream = rsi(tStream.stream,n);
        chain.doTransform(tStream);
    }
}
