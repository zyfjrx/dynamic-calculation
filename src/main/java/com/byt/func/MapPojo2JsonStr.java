package com.byt.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @title: pojo class to json string
 * @author: zhangyifan
 * @date: 2022/7/6 09:10
 */
public class MapPojo2JsonStr<T> implements MapFunction<T,String> {
    @Override
    public String map(T t) throws Exception {
        String jsonString = JSON.toJSONString(t);
        //System.out.println(jsonString);
        return jsonString;
    }
}
