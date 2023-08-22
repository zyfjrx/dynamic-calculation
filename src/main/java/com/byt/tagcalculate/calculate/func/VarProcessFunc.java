package com.byt.tagcalculate.calculate.func;

import com.byt.common.utils.BytTagUtil;
import com.byt.tagcalculate.pojo.TagKafkaInfo;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.math.BigDecimal;
import java.util.LinkedList;
import java.util.Queue;

/**
 * @title: VAR算子函数
 * @author: zhangyf
 * @date: 2023/7/5 14:04
 **/
public class VarProcessFunc extends KeyedProcessFunction<String, TagKafkaInfo, TagKafkaInfo> {
    private OutputTag<TagKafkaInfo> dwdOutPutTag;
    private MapState<String, Queue<TagKafkaInfo>> mapState;

    public VarProcessFunc(OutputTag<TagKafkaInfo> dwdOutPutTag) {
        this.dwdOutPutTag = dwdOutPutTag;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        mapState = getRuntimeContext().getMapState(
                new MapStateDescriptor<String, Queue<TagKafkaInfo>>(
                        "varMap",
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
        if (!mapState.contains(key)) {
            Queue<TagKafkaInfo> varQueue = new LinkedList<>();
            varQueue.offer(value);
            mapState.put(key, varQueue);
        } else {
            mapState.get(key).offer(value);
        }

        if (mapState.get(key).size() > nBefore){
            TagKafkaInfo firstTag = mapState.get(key).poll();
            BigDecimal firstTagValue = firstTag.getValue();
//            TagKafkaInfo newTag = new TagKafkaInfo();
//            BeanUtils.copyProperties(newTag, value);
            try {
                BigDecimal diffValue = value.getValue().subtract(firstTagValue);
                value.setValue(diffValue);
            } catch (Exception e) {
                value.setValue(null);
                e.printStackTrace();
            }
            BytTagUtil.outputByKeyed(value,ctx,out,dwdOutPutTag);
        }



       /* lastQueue.offer(value);
        int size = lastQueue.size();
        if (size > nBefore) {
            TagKafkaInfo firstTag = lastQueue.poll();
            BigDecimal firstTagValue = firstTag.getValue();
            TagKafkaInfo newTag = new TagKafkaInfo();
            BeanUtils.copyProperties(newTag, value);
            try {
                BigDecimal diffValue = value.getValue().subtract(firstTagValue);
                newTag.setValue(diffValue);
            } catch (Exception e) {
                newTag.setValue(null);
                e.printStackTrace();
            }
            BytTagUtil.outputByKeyed(newTag,ctx,out,dwdOutPutTag);*/
    }
}
