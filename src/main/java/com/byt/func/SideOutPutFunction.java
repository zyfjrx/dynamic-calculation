package com.byt.func;

import com.byt.pojo.TagKafkaInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.util.HashMap;

/**
 * @title:
 * @author: zhangyifan
 * @date: 2022/7/4 15:19
 */
public class SideOutPutFunction extends ProcessFunction<TagKafkaInfo, TagKafkaInfo> {
    private HashMap<String, OutputTag<TagKafkaInfo>> sideOutPutTags;
    private OutputTag<TagKafkaInfo> errorSide;
    private SimpleDateFormat sdf1;
    private SimpleDateFormat sdf2;

    public SideOutPutFunction(HashMap<String, OutputTag<TagKafkaInfo>> sideOutPutTags, OutputTag<TagKafkaInfo> errorSide) {
        this.sideOutPutTags = sideOutPutTags;
        this.errorSide = errorSide;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        sdf2 = new SimpleDateFormat("yyyyMMddHHmmss");
    }

    @Override
    public void processElement(TagKafkaInfo value, ProcessFunction<TagKafkaInfo, TagKafkaInfo>.Context ctx, Collector<TagKafkaInfo> out) throws Exception {
        String calculate = getCalculateParamByPOJO(value);
        value.setTime(sdf1.format(sdf2.parse(value.getTime())));
        if (sideOutPutTags.containsKey(calculate)) {
            ctx.output(sideOutPutTags.getOrDefault(calculate, errorSide), value);
        } else {
            out.collect(value);
        }
    }


    public String getCalculateParamByPOJO(TagKafkaInfo data) {
        // return data.getCalculateType() + "#" + data.getTimeGap() + "#" + data.getSlideGap();
        return data.getCalculateType() + "#" + data.getCalculateParam();

    }
}
