package com.byt.tagcalculate.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import org.apache.flink.api.common.functions.MapFunction;

public class BeanToJsonStrFunction<T> implements MapFunction<T, String> {
    @Override
    public String map(T bean) throws Exception {
        SerializeConfig config = new SerializeConfig();
        config.propertyNamingStrategy = PropertyNamingStrategy.SnakeCase;
        return  JSON.toJSONString(bean, config);
    }
}
