package com.byt.func;

import com.byt.pojo.TagKafkaInfo;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;

/**
 * @title: 分流预处理和最终结果
 * @author: zhangyifan
 * @date: 2022/7/6 09:39
 */
public class PreOrSecondResultFunction extends ProcessFunction<TagKafkaInfo,TagKafkaInfo> {
    private OutputTag<TagKafkaInfo> preOutPutTag;
    private OutputTag<TagKafkaInfo> secondOutPutTag;
    private SimpleDateFormat sdf;

    public PreOrSecondResultFunction(OutputTag<TagKafkaInfo> preOutPutTag, OutputTag<TagKafkaInfo> secondOutPutTag) {
        this.preOutPutTag = preOutPutTag;
        this.secondOutPutTag = secondOutPutTag;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    }

    @Override
    public void processElement(TagKafkaInfo tagKafkaInfo, ProcessFunction<TagKafkaInfo, TagKafkaInfo>.Context context, Collector<TagKafkaInfo> collector) throws Exception {
        //System.out.println("time:"+tagKafkaInfo.getTimeGap()+tagKafkaInfo);
        if (tagKafkaInfo.getBytName().endsWith("_$")) {
            context.output(preOutPutTag, tagKafkaInfo);
        } else if (tagKafkaInfo.getCalculateParam().contains("s")){
            //System.out.println("s----"+tagKafkaInfo);
            context.output(secondOutPutTag,tagKafkaInfo);
        }else {
            //System.out.println("aaa:"+tagKafkaInfo);
            collector.collect(tagKafkaInfo);
        }
    }
}
