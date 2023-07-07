package com.byt.tagcalculate.func;

import com.byt.tagcalculate.pojo.PostSetup;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.util.HashSet;

/**
 * @title: 广播描述符
 * @author: zhangyifan
 * @date: 2022/6/27 16:13
 */
public class Descriptors {
    public static final MapStateDescriptor<String, PostSetup> postMapDescriptor =
            new MapStateDescriptor<>("post params", BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(PostSetup.class));

    public static final MapStateDescriptor<String, HashSet<String>> postDimDescriptor =
            new MapStateDescriptor<>("post dim", BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(new TypeHint<HashSet<String>>() {
            }));
}
