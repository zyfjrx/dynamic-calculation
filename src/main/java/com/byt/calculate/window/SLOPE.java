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
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * @title: 斜率计算算子
 * @author: zhangyifan
 * @date: 2022/8/29 09:57
 */
public class SLOPE implements Transform {
    private String size;
    private String slide;

    public SLOPE(List<String> params) {
        this.size = params.get(0);
        this.slide = params.get(1);
    }

    public DataStream<TagKafkaInfo> slope(DataStream<TagKafkaInfo> in, String size, String slide) {
        return in
                .keyBy(new KeySelector<TagKafkaInfo, Tuple3<String, String, Integer>>() {
                    @Override
                    public Tuple3<String, String, Integer> getKey(TagKafkaInfo tagKafkaInfo) throws Exception {
                        return Tuple3.of(
                                tagKafkaInfo.getTopic(),
                                tagKafkaInfo.getBytName(),
                                tagKafkaInfo.getLineId()
                        );
                    }
                })
                .window(WindowTimeSelector.getWindowTime(TimeParams.timeParams(size), TimeParams.timeParams(slide)))
                .process(new SlopeProcessFunction())
                .setParallelism(ConfigManager.getInteger(PropertiesConstants.STREAM_CALCULATE_PARALLELISM))
                .name("SLOPE-斜率-window(" + size + "," + slide + ")");
    }

    @Override
    public void doTransForm(TStream tStream, TransformChain chain) {
        tStream.stream = slope(tStream.stream, size, slide);
        chain.doTransform(tStream);
    }


    class SlopeProcessFunction extends ProcessWindowFunction<TagKafkaInfo, TagKafkaInfo, Tuple3<String, String, Integer>, TimeWindow> {
        private SimpleDateFormat sdf;

        @Override
        public void open(Configuration parameters) throws Exception {
            sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }

        @Override
        public void process(Tuple3<String, String, Integer> key, Context context, Iterable<TagKafkaInfo> iterable, Collector<TagKafkaInfo> collector) throws Exception {
            Iterator<TagKafkaInfo> tagIterator = iterable.iterator();
            ArrayList<TagKafkaInfo> list = new ArrayList<>();

            BigDecimal sum_y = BigDecimal.ZERO;
            while (tagIterator.hasNext()) {
                TagKafkaInfo tagKafkaInfo = tagIterator.next();
                list.add(tagKafkaInfo);
                if (tagKafkaInfo.getValue() != null) {
                    sum_y = sum_y.add(tagKafkaInfo.getValue());
                }
            }
            list.sort(new Comparator<TagKafkaInfo>() {
                @Override
                public int compare(TagKafkaInfo o1, TagKafkaInfo o2) {
                    return (int) (o1.getTimestamp() - o2.getTimestamp());
                }
            });
            TagKafkaInfo tagKafkaInfo = list.get(0);

            try {
                BigDecimal num_y = new BigDecimal(list.size());
                BigDecimal avg_y = sum_y.divide(num_y, 4, BigDecimal.ROUND_HALF_UP);
                BigDecimal avg_x = new BigDecimal(list.size() + 1L).divide(new BigDecimal(2), 4, BigDecimal.ROUND_HALF_UP);
                System.out.println(list.size() + "------" + avg_x);
                BigDecimal dividend = BigDecimal.ZERO;
                BigDecimal divisor = BigDecimal.ZERO;
                for (int i = 0; i < list.size(); i++) {
                    BigDecimal xi = new BigDecimal(i + 1);
                    BigDecimal yi = list.get(i).getValue();
                    dividend = dividend.add(xi.subtract(avg_x).multiply(yi.subtract(avg_y)));
                    divisor = divisor.add(xi.subtract(avg_x).pow(2));
                }

                System.out.println("slope-------------------------------1");
                BigDecimal slope = dividend.divide(divisor, 4, BigDecimal.ROUND_HALF_UP);
                System.out.println("slope-------------------------------2");
                tagKafkaInfo.setValue(slope);
            } catch (ArithmeticException | NullPointerException e) {
                tagKafkaInfo.setValue(null);
                System.out.println("SLOPE 计算异常～～～～");
            }
            tagKafkaInfo.setTime(sdf.format(context.window().getEnd()));
            //tagKafkaInfo.setTimestamp(null);
            collector.collect(tagKafkaInfo);
            list.clear();
        }
    }


}
