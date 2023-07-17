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
import java.util.Iterator;

/**
 * @title: CV算子函数(原始数据标准差与平均值的比)
 * @author: zhangyf
 * @date: 2023/7/6 14:38
 **/
public class CvProcessFunc extends ProcessWindowFunction<TagKafkaInfo, TagKafkaInfo, String, TimeWindow> {
    private  SimpleDateFormat sdf;
    private OutputTag<TagKafkaInfo> dwdOutPutTag;

    public CvProcessFunc(OutputTag<TagKafkaInfo> dwdOutPutTag) {
        this.dwdOutPutTag = dwdOutPutTag;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    }

    @Override
    public void process(String key, ProcessWindowFunction<TagKafkaInfo, TagKafkaInfo, String, TimeWindow>.Context context, Iterable<TagKafkaInfo> elements, Collector<TagKafkaInfo> out) throws Exception {
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
        TagKafkaInfo tagKafkaInfo = elements.iterator().next();;
        try {
            variance = variance.divide(num, 4, BigDecimal.ROUND_HALF_UP);
            Double std = Math.sqrt(variance.doubleValue());
            Double cv = std / avg.doubleValue();
            tagKafkaInfo.setValue(new BigDecimal(cv).setScale(4, BigDecimal.ROUND_HALF_UP));
        } catch (Exception e) {
            System.out.println("CV 计算异常～～～～");
            tagKafkaInfo.setValue(null);
        }
        tagKafkaInfo.setTime(sdf.format(context.window().getEnd()));
        BytTagUtil.outputByWindow(tagKafkaInfo,context,out,dwdOutPutTag);
        list.clear();
    }
}
