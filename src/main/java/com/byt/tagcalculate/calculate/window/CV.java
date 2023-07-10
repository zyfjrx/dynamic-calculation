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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @title: 变异系数：原始数据标准差与平均值的比
 * @author: zhangyifan
 * @date: 2022/8/25 13:49
 */
public class CV implements Transform {
    private String size;
    private String slide;

    public CV(List<String> params) {
        this.size = params.get(0);
        this.slide = params.get(1);
    }

    public DataStream<TagKafkaInfo> cv(DataStream<TagKafkaInfo> in, String size, String slide) {
        return in
                .keyBy(new KeySelector<TagKafkaInfo, Tuple3<String,String, Integer>>() {
                    @Override
                    public Tuple3<String,String, Integer> getKey(TagKafkaInfo value) throws Exception {
                        return Tuple3.of(
                                value.getTopic(),
                                value.getBytName(),
                                value.getLineId()
                        );
                    }
                })
                .window(WindowTimeSelector.getWindowTime(TimeParams.timeParams(size), TimeParams.timeParams(slide)))
                .process(new CVProcessFunc())
                .setParallelism(ConfigManager.getInteger(PropertiesConstants.STREAM_CALCULATE_PARALLELISM))
                .name("CV-变异系数-window("+size+","+slide+")");
    }

    @Override
    public void doTransForm(TStream tStream, TransformChain chain) {
        tStream.stream = cv(tStream.stream, size, slide);
        chain.doTransform(tStream);
    }


    public class CVProcessFunc extends ProcessWindowFunction<TagKafkaInfo, TagKafkaInfo, Tuple3<String,String, Integer>, TimeWindow> {
        private SimpleDateFormat sdf;

        @Override
        public void open(Configuration parameters) throws Exception {
            sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }

        @Override
        public void process(Tuple3<String,String, Integer> tuple3, ProcessWindowFunction<TagKafkaInfo, TagKafkaInfo, Tuple3<String,String, Integer>, TimeWindow>.Context context, Iterable<TagKafkaInfo> elements, Collector<TagKafkaInfo> out) throws Exception {
            BigDecimal sum = new BigDecimal(0);
            BigDecimal num = new BigDecimal(0);
            ArrayList<TagKafkaInfo> list = new ArrayList<>();
            Iterator<TagKafkaInfo> iterator1 = elements.iterator();
            while (iterator1.hasNext()) {
                TagKafkaInfo data = iterator1.next();
                sum = sum.add(data.getValue());
                num = num.add(BigDecimal.valueOf(1L));
                list.add(data);
            }
            BigDecimal avg = sum.divide(num, 4, BigDecimal.ROUND_HALF_UP);
            BigDecimal variance = new BigDecimal(0);

            for (TagKafkaInfo tagKafkaInfo : list) {
                variance = variance.add(tagKafkaInfo.getValue().subtract(avg).pow(2));
            }


           /*
           Iterator<TagKafkaInfo> iterator2 = elements.iterator();
           while (iterator2.hasNext()) {
                TagKafkaInfo data = iterator2.next();
                variance = variance.add(data.getValue().subtract(avg).pow(2));
            }*/
            TagKafkaInfo tagKafkaInfo = null;
            try {
                variance = variance.divide(num, 4, BigDecimal.ROUND_HALF_UP);
                Double std = Math.sqrt(variance.doubleValue());
                Double cv = std / avg.doubleValue();
                tagKafkaInfo = elements.iterator().next();
                tagKafkaInfo.setValue(new BigDecimal(cv).setScale(4, BigDecimal.ROUND_HALF_UP));
                tagKafkaInfo.setTime(sdf.format(context.window().getEnd()));
            } catch (Exception e) {
                System.out.println("CV 计算异常～～～～");
                tagKafkaInfo.setValue(null);
                tagKafkaInfo.setTime(sdf.format(context.window().getEnd()));
            }
            //tagKafkaInfo.setTimeGap(null);
            tagKafkaInfo.setTimestamp(null);
            list.clear();
            out.collect(tagKafkaInfo);
        }
    }

}
