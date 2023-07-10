package com.byt.tagcalculate.calculate.window;

import com.byt.tagcalculate.calculate.calculatechain.*;
import com.byt.tagcalculate.constants.PropertiesConstants;
import com.byt.tagcalculate.pojo.TagKafkaInfo;
import com.byt.common.utils.ConfigManager;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.List;

/**
 * @title: 最接近计算时刻的数据点取值
 * @author: zhangyifan
 * @date: 2022/8/22 10:47
 */
public class INTERP implements Transform {

    private String size;
    private String slide;

    public INTERP(List<String> params) {
        this.size = params.get(0);
        this.slide = params.get(1);
    }

    public INTERP() {
    }

    public DataStream<TagKafkaInfo> interpolation(DataStream<TagKafkaInfo> in, String size, String slide) {
        return in
                .keyBy(new KeySelector<TagKafkaInfo, Tuple3<String,String, Integer>>() {
                    @Override
                    public Tuple3<String,String, Integer> getKey(TagKafkaInfo data) throws Exception {
                        return Tuple3.of(
                                data.getTopic(),
                                data.getBytName(),
                                data.getLineId()
                        );
                    }
                })
                .window(WindowTimeSelector.getWindowTime(TimeParams.timeParams(size), TimeParams.timeParams(slide)))
                .reduce(new ReduceFunction<TagKafkaInfo>() {
                            @Override
                            public TagKafkaInfo reduce(TagKafkaInfo t1, TagKafkaInfo t2) throws Exception {
                                TagKafkaInfo tagKafkaInfo = t1.getTimestamp() > t2.getTimestamp() ? t1 : t2;
                                return tagKafkaInfo;
                            }
                        },
                        new ProcessWindowFunction<TagKafkaInfo, TagKafkaInfo, Tuple3<String,String, Integer>, TimeWindow>() {
                            private SimpleDateFormat sdf;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                            }

                            @Override
                            public void process(Tuple3<String,String, Integer> tuple2, ProcessWindowFunction<TagKafkaInfo, TagKafkaInfo, Tuple3<String,String, Integer>, TimeWindow>.Context context, Iterable<TagKafkaInfo> iterable, Collector<TagKafkaInfo> collector) throws Exception {
                                TagKafkaInfo tagKafkaInfo = iterable.iterator().next();
                                tagKafkaInfo.setTime(sdf.format(context.window().getEnd()));
                                //tagKafkaInfo.setTime(null);
                                collector.collect(tagKafkaInfo);
                            }
                        }
                )
                .setParallelism(ConfigManager.getInteger(PropertiesConstants.STREAM_CALCULATE_PARALLELISM))
                .name("INTERP-插值-window("+size+","+slide+")");
    }


    @Override
    public void doTransForm(TStream tStream, TransformChain chain) {
        tStream.stream = interpolation(tStream.stream, size, slide);
        chain.doTransform(tStream);
    }
}
