package com.byt.tagcalculate.calculate.func;

import com.byt.common.utils.BytTagUtil;
import com.byt.tagcalculate.pojo.TagKafkaInfo;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.LinkedList;
import java.util.Queue;

/**
 * @title: LAST算子函数
 * @author: zhangyf
 * @date: 2023/7/5 14:04
 **/
public class LastProcessFunc extends KeyedProcessFunction<String, TagKafkaInfo, TagKafkaInfo> {
    private  OutputTag<TagKafkaInfo> dwdOutPutTag;
    private MapState<String , Queue<TagKafkaInfo>> mapState;

    public LastProcessFunc(OutputTag<TagKafkaInfo> dwdOutPutTag) {
        this.dwdOutPutTag = dwdOutPutTag;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        mapState = getRuntimeContext().getMapState(
                new MapStateDescriptor<String, Queue<TagKafkaInfo>>(
                        "lastMap",
                        Types.STRING,
                        TypeInformation.of(new TypeHint<Queue<TagKafkaInfo>>() {
                        })
                )
        );
    }

    @Override
    public void processElement(TagKafkaInfo value, KeyedProcessFunction<String, TagKafkaInfo, TagKafkaInfo>.Context ctx, Collector<TagKafkaInfo> out) throws Exception {
        Integer nBefore = value.getCurrNBefore();
        String key = value.getBytName();
        if (!mapState.contains(key)){
            Queue<TagKafkaInfo> lastQueue = new LinkedList<>();
            lastQueue.offer(value);
            mapState.put(key,lastQueue);
        } else {
            mapState.get(key).offer(value);
        }
        if (mapState.get(key).size() > nBefore){
            TagKafkaInfo tagKafkaInfo = mapState.get(key).poll();
            // 存在oom风险
 //           BigDecimal tagKafkaInfoValue = tagKafkaInfo.getValue();
//            TagKafkaInfo newTag = new TagKafkaInfo();
//            BeanUtils.copyProperties(newTag, value);
//            newTag.setValue(tagKafkaInfoValue);
            tagKafkaInfo.setTime(value.getTime());
            BytTagUtil.outputByKeyed(tagKafkaInfo,ctx,out,dwdOutPutTag);
        }
    }
}
