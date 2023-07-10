package com.byt.tagcalculate.calculate.window;

import com.byt.tagcalculate.calculate.calculatechain.TStream;
import com.byt.tagcalculate.calculate.calculatechain.Transform;
import com.byt.tagcalculate.calculate.calculatechain.TransformChain;
import com.byt.tagcalculate.constants.PropertiesConstants;
import com.byt.tagcalculate.pojo.TagKafkaInfo;
import com.byt.common.utils.ConfigManager;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @title: 趋势算子
 * @author: zhangyifan
 * @date: 2022/8/30 14:49
 */
public class TREND implements Transform {
    private Integer nBefore;


    public TREND(List<String> params) {
        this.nBefore = Integer.parseInt(params.get(0));
    }

    public DataStream<TagKafkaInfo> trend(DataStream<TagKafkaInfo> in, Integer nBefore) {
        KeyedStream<TagKafkaInfo, Tuple3<String, String, Integer>> keyedStream = in
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TagKafkaInfo>forBoundedOutOfOrderness(Duration.ofSeconds(1L))
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<TagKafkaInfo>() {
                                            @Override
                                            public long extractTimestamp(TagKafkaInfo tagKafkaInfo, long l) {
                                                return tagKafkaInfo.getTimestamp();
                                            }
                                        }
                                )
                )
                .keyBy(new KeySelector<TagKafkaInfo, Tuple3<String, String, Integer>>() {
                    @Override
                    public Tuple3<String, String, Integer> getKey(TagKafkaInfo tagKafkaInfo) throws Exception {
                        return Tuple3.of(
                                tagKafkaInfo.getTopic(),
                                tagKafkaInfo.getBytName(),
                                tagKafkaInfo.getLineId()
                        );
                    }
                });

        // 定义匹配规则
        Pattern<TagKafkaInfo, TagKafkaInfo> pattern = Pattern.<TagKafkaInfo>begin("first");
        for (int i = 1; i <= nBefore; i++) {
            pattern = pattern.next(String.valueOf(i));
        }
        System.out.println(pattern);
        // 规则作用流上
        PatternStream<TagKafkaInfo> patternStream = CEP.pattern(keyedStream, pattern);
        // 提取匹配数据
        return patternStream.select(new PatternSelectFunction<TagKafkaInfo, TagKafkaInfo>() {
                    @Override
                    public TagKafkaInfo select(Map<String, List<TagKafkaInfo>> map) throws Exception {
                        TagKafkaInfo first = map.get("first").get(0);
                        TagKafkaInfo second = map.get(String.valueOf(nBefore)).get(0);
                        //System.out.println("f->" + first);
                        //System.out.println("s->" + second);
                        TagKafkaInfo newTag = new TagKafkaInfo();

                        BeanUtils.copyProperties(newTag, second);
                        try {
                            BigDecimal trendValue = second.getValue().divide(first.getValue(), 4, BigDecimal.ROUND_HALF_UP);
                            newTag.setValue(trendValue);
                        } catch (Exception e) {
                            newTag.setValue(null);
                            e.printStackTrace();
                        }
                        return newTag;
                    }
                })
                .setParallelism(ConfigManager.getInteger(PropertiesConstants.STREAM_CALCULATE_PARALLELISM))
                .name("TREND-趋势");
    }

    @Override
    public void doTransForm(TStream tStream, TransformChain chain) {
        tStream.stream = trend(tStream.stream, nBefore);
        chain.doTransform(tStream);
    }
}
