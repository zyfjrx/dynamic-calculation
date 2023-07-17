package com.byt.tagcalculate.calculate.func;

import com.byt.tagcalculate.pojo.TagKafkaInfo;
import com.byt.common.utils.BytTagUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;

/**
 * @title: SLOPE算子函数
 * @author: zhangyf
 * @date: 2023/7/7 14:27
 **/
public class SlopeProcessFunc extends ProcessWindowFunction<TagKafkaInfo, TagKafkaInfo, String, TimeWindow> {
    private  SimpleDateFormat sdf;
    private  OutputTag<TagKafkaInfo> dwdOutPutTag;

    public SlopeProcessFunc(OutputTag<TagKafkaInfo> dwdOutPutTag) {
        this.dwdOutPutTag = dwdOutPutTag;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    }

    @Override
    public void process(String key, ProcessWindowFunction<TagKafkaInfo, TagKafkaInfo, String, TimeWindow>.Context context, Iterable<TagKafkaInfo> elements, Collector<TagKafkaInfo> out) throws Exception {
        Iterator<TagKafkaInfo> tagIterator = elements.iterator();
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
            BigDecimal slope = dividend.divide(divisor, 4, BigDecimal.ROUND_HALF_UP);
            tagKafkaInfo.setValue(slope);
        } catch (ArithmeticException | NullPointerException e) {
            tagKafkaInfo.setValue(null);
            System.out.println("SLOPE 计算异常～～～～");
        }
        tagKafkaInfo.setTime(sdf.format(context.window().getEnd()));
        BytTagUtil.outputByWindow(tagKafkaInfo,context,out,dwdOutPutTag);
        list.clear();
    }
}
