package com.byt.func;

import com.byt.pojo.TagKafkaInfo;
import org.apache.flink.api.common.functions.FilterFunction;

/**
 * @title:
 * @author: zhangyifan
 * @date: 2022/9/6 09:26
 */
public class FilterNormalAndJobName implements FilterFunction<TagKafkaInfo> {
    private String jobNames;

    public FilterNormalAndJobName(String jobName) {
        this.jobNames = jobName;
    }

    @Override
    public boolean filter(TagKafkaInfo tagKafkaInfo) throws Exception {

        // todo 通过产线id过滤对应自己产线的计算数据



        //System.out.println(tagKafkaInfo);
        // dev
        return tagKafkaInfo.getIsNormal().equals(1) && jobNames.contains(tagKafkaInfo.getTaskName());
    }
}
