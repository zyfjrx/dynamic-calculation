package com.byt.calculate.window;

import com.byt.calculate.*;
import com.byt.constants.PropertiesConstants;
import com.byt.pojo.TagKafkaInfo;
import com.byt.utils.ConfigManager;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * @title: 中位数算子
 * @author: zhangyifan
 * @date: 2022/7/7 14:09
 */
public class MEDIAN implements Transform {
    private String size;
    private String slide;

    public MEDIAN(List<String> params) {
        this.size = params.get(0);
        this.slide = params.get(1);
    }

    public DataStream<TagKafkaInfo> median(DataStream<TagKafkaInfo> in, String size, String slide) {
        return in
                .keyBy(new KeySelector<TagKafkaInfo, Tuple3<String,String, Integer>>() {
                    @Override
                    public Tuple3<String,String, Integer> getKey(TagKafkaInfo data) throws Exception {
                        return Tuple3.of(data.getTopic(), data.getBytName(),data.getLineId());
                    }
                })
                .window(WindowTimeSelector.getWindowTime(TimeParams.timeParams(size),TimeParams.timeParams(slide)))
                .process(new MedianProcessFunction())
                .setParallelism(ConfigManager.getInteger(PropertiesConstants.STREAM_CALCULATE_PARALLELISM))
                .name("MEDIAN-window("+size+","+slide+")");
    }


    @Override
    public void doTransForm(TStream tStream, TransformChain chain) {
        tStream.stream = median(tStream.stream,size,slide);
        chain.doTransform(tStream);
    }


    class MedianProcessFunction extends ProcessWindowFunction<TagKafkaInfo, TagKafkaInfo, Tuple3<String,String, Integer>, TimeWindow> {
        private SimpleDateFormat sdf;

        @Override
        public void open(Configuration parameters) throws Exception {
            sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }

        @Override
        public void process(Tuple3<String,String, Integer> tuple2, ProcessWindowFunction<TagKafkaInfo, TagKafkaInfo, Tuple3<String,String, Integer>, TimeWindow>.Context context, Iterable<TagKafkaInfo> iterable, Collector<TagKafkaInfo> collector) throws Exception {
            ArrayList<BigDecimal> sortedValue = new ArrayList<>();
            Iterator<TagKafkaInfo> iterator = iterable.iterator();
            iterator.forEachRemaining(tag -> {
                sortedValue.add(tag.getValue());
            });
            sortedValue.sort(new Comparator<BigDecimal>() {
                @Override
                public int compare(BigDecimal o1, BigDecimal o2) {
                    return o1.intValue() - o2.intValue();
                }
            });
            BigDecimal median = new BigDecimal(0L);
            if (sortedValue.size() % 2 == 0) {
                median = sortedValue.get(sortedValue.size() / 2 - 1)
                        .add(sortedValue.get(sortedValue.size() / 2))
                        .divide(BigDecimal.valueOf(2L), 4, BigDecimal.ROUND_HALF_UP);
            } else {
                median = sortedValue.get((sortedValue.size() + 1) / 2 - 1);
            }
            Iterator<TagKafkaInfo> iterator1 = iterable.iterator();

            if (iterator1.hasNext()) {
                TagKafkaInfo tagKafkaInfo = iterator1.next();
                tagKafkaInfo.setValue(median);
                tagKafkaInfo.setTime(sdf.format(context.window().getEnd()));
                //tagKafkaInfo.setTimestamp(null);
                collector.collect(tagKafkaInfo);
            }
        }
    }
}
