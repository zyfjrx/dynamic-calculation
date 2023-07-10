package com.byt.tagcalculate.func;

import com.byt.tagcalculate.pojo.TagKafkaInfo;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @title: 分流预处理和最终结果
 * @author: zhangyifan
 * @date: 2022/7/6 09:39
 */
public class PreOrSecondResultFunction extends ProcessFunction<TagKafkaInfo,TagKafkaInfo> {
    private OutputTag<TagKafkaInfo> preOutPutTag;
    private OutputTag<TagKafkaInfo> secondOutPutTag;

    public PreOrSecondResultFunction(OutputTag<TagKafkaInfo> preOutPutTag, OutputTag<TagKafkaInfo> secondOutPutTag) {
        this.preOutPutTag = preOutPutTag;
        this.secondOutPutTag = secondOutPutTag;
    }

    @Override
    public void processElement(TagKafkaInfo tagKafkaInfo, ProcessFunction<TagKafkaInfo, TagKafkaInfo>.Context context, Collector<TagKafkaInfo> collector) throws Exception {
        if (tagKafkaInfo.getBytName().endsWith("_$")) {
            context.output(preOutPutTag, tagKafkaInfo);
        } else if (tagKafkaInfo.getCalculateParam().contains("s")){
            context.output(secondOutPutTag,tagKafkaInfo);
        }else {
            collector.collect(tagKafkaInfo);
        }
    }
}
