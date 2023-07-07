package test;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.LinkedList;
import java.util.Queue;

/**
 * @title:
 * @author: zhangyf
 * @date: 2023/3/21 11:05
 **/
public class RSI {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .socketTextStream("localhost", 9999)
                .map(new MapFunction<String, Double>() {
                    @Override
                    public Double map(String s) throws Exception {
                        return Double.parseDouble(s);
                    }
                })
                .keyBy(r -> 1)
                .flatMap(new RichFlatMapFunction<Double, Double>() {
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
                    public void flatMap(Double aDouble, Collector<Double> collector) throws Exception {
                        if (prevClosePrice.value() == null){
                            gainQueue.offer(0.0);
                            lossQueue.offer(0.0);
                        }
                        if (prevClosePrice.value() != null) {
                            double gain = Math.max(aDouble - prevClosePrice.value(), 0);
                            double loss = Math.max(prevClosePrice.value() - aDouble, 0);
                            gainQueue.offer(gain);
                            lossQueue.offer(loss);
                        }
                        prevClosePrice.update(aDouble);

                        if (gainQueue.size() > 13 && lossQueue.size() > 13) {
                            double avgGain = gainQueue.stream().mapToDouble(Double::doubleValue).average().getAsDouble();
                            double avgLoss = lossQueue.stream().mapToDouble(Double::doubleValue).average().getAsDouble();
                            System.out.println("avgGain"+avgGain);
                            System.out.println("avgLoss"+avgLoss);
                            System.out.println("queue1: "+gainQueue);
                            System.out.println("queue2: "+lossQueue);
                            double rs = avgGain / avgLoss;
                            double rsi = 100 - (100 / (1 + rs));
                            gainQueue.poll();
                            lossQueue.poll();
                            collector.collect(rsi);
                        }

                    }
                })
                .print();


        env.execute();
    }
}
