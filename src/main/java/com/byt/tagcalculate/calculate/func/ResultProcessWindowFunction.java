package com.byt.tagcalculate.calculate.func;

import com.byt.common.utils.BytTagUtil;
import com.byt.tagcalculate.pojo.TagKafkaInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;

/**
 * @title:
 * @author: zhangyf
 * @date: 2023/7/24 13:56
 **/
public class ResultProcessWindowFunction extends ProcessWindowFunction<TagKafkaInfo, TagKafkaInfo, String, TimeWindow> {
    private SimpleDateFormat sdf;
    private OutputTag<TagKafkaInfo> dwdOutPutTag;

    public ResultProcessWindowFunction(OutputTag<TagKafkaInfo> dwdOutPutTag) {
        this.dwdOutPutTag = dwdOutPutTag;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    }

    @Override
    public void process(String key, ProcessWindowFunction<TagKafkaInfo, TagKafkaInfo, String, TimeWindow>.Context context, Iterable<TagKafkaInfo> iterable, Collector<TagKafkaInfo> collector) throws Exception {
        TagKafkaInfo tagKafkaInfo = iterable.iterator().next();
        tagKafkaInfo.setTime(sdf.format(context.window().getEnd()));
        BytTagUtil.outputByWindow(tagKafkaInfo,context,collector,dwdOutPutTag);
    }
}
