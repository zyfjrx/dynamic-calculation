package com.byt.tagcalculate.calculate.window;

import com.byt.tagcalculate.calculate.calculatechain.*;
import com.byt.tagcalculate.constants.PropertiesConstants;
import com.byt.tagcalculate.pojo.TagKafkaInfo;
import com.byt.common.utils.ConfigManager;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.List;

/**
 * @title: 标准差算子
 * @author: zhangyifan
 * @date: 2022/8/24 09:56
 */
public class STD implements Transform {
    private String size;
    private String slide;

    public STD(List<String> params) {
        this.size = params.get(0);
        this.slide = params.get(1);
    }

    public DataStream<TagKafkaInfo> std(DataStream<TagKafkaInfo> in, String size, String slide) {
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
                .window(WindowTimeSelector.getWindowTime(TimeParams.timeParams(size), TimeParams.timeParams(slide)))
                .process(new stdProcessFunction())
                .setParallelism(ConfigManager.getInteger(PropertiesConstants.STREAM_CALCULATE_PARALLELISM))
                .name("STD-标准差-window("+size+","+slide+")");
    }

    @Override
    public void doTransForm(TStream tStream, TransformChain chain) {
        tStream.stream = std(tStream.stream, size, slide);
        chain.doTransform(tStream);
    }


    class stdProcessFunction extends ProcessWindowFunction<TagKafkaInfo, TagKafkaInfo, Tuple3<String,String, Integer>, TimeWindow> {
        private SimpleDateFormat sdf;

        @Override
        public void open(Configuration parameters) throws Exception {
            sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }

        @Override
        public void process(Tuple3<String,String, Integer> tuple2, ProcessWindowFunction<TagKafkaInfo, TagKafkaInfo, Tuple3<String,String, Integer>, TimeWindow>.Context context, Iterable<TagKafkaInfo> iterable, Collector<TagKafkaInfo> collector) throws Exception {
            BigDecimal sum = new BigDecimal(0);
            BigDecimal num = new BigDecimal(0);
            Iterator<TagKafkaInfo> iterator1 = iterable.iterator();
            while (iterator1.hasNext()) {
                TagKafkaInfo info = iterator1.next();
                sum = sum.add(info.getValue());
                num = num.add(new BigDecimal(1));
            }
            BigDecimal avg = sum.divide(num, 4, BigDecimal.ROUND_HALF_UP);
            BigDecimal variance = new BigDecimal(0);
            Iterator<TagKafkaInfo> iterator2 = iterable.iterator();
            while (iterator2.hasNext()) {
                TagKafkaInfo info = iterator2.next();
                variance = variance.add(info.getValue().subtract(avg).pow(2));
            }
            variance = variance.divide(num, 4, BigDecimal.ROUND_HALF_UP);
            Double std = Math.sqrt(variance.doubleValue());
            TagKafkaInfo tagKafkaInfo = iterable.iterator().next();
            tagKafkaInfo.setTime(sdf.format(context.window().getEnd()));
            //tagKafkaInfo.setTimestamp(null);
            tagKafkaInfo.setValue(new BigDecimal(std).setScale(4, BigDecimal.ROUND_HALF_UP));
            //System.out.println(tagKafkaInfo);
            System.out.println("result:"+tagKafkaInfo);
            collector.collect(tagKafkaInfo);
        }
    }
}
