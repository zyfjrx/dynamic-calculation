package com.byt.tagcalculate.calculate.func;

import com.byt.common.utils.BytTagUtil;
import com.byt.tagcalculate.pojo.TagKafkaInfo;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * @title:
 * @author: zhangyf
 * @date: 2023/7/17 12:37
 **/
public class CepPatternProcessFunc extends PatternProcessFunction<TagKafkaInfo, TagKafkaInfo> {
    private OutputTag<TagKafkaInfo> dwdOutPutTag;

    public CepPatternProcessFunc(OutputTag<TagKafkaInfo> dwdOutPutTag) {
        this.dwdOutPutTag = dwdOutPutTag;
    }

    @Override
    public void processMatch(Map<String, List<TagKafkaInfo>> map, Context context, Collector<TagKafkaInfo> collector) throws Exception {
        TagKafkaInfo first = map.get("tag").get(0);
        Integer currNBefore = first.getCurrNBefore();
        TagKafkaInfo second;
        try {
            second = map.get("tag").get(currNBefore);
        } catch (Exception e) {
            System.out.println("cep error");
            List<TagKafkaInfo> tag = map.get("tag");
            second = tag.get(tag.size() - 1);
        }
        TagKafkaInfo newTag = new TagKafkaInfo();
        BeanUtils.copyProperties(newTag, second);
        newTag.setValue(first.getValue());
        BytTagUtil.outputByKeyed(newTag, context, collector, dwdOutPutTag);
    }
}
