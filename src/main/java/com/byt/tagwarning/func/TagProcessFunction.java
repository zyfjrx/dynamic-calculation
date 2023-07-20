package com.byt.tagwarning.func;


import com.byt.tagcalculate.pojo.TagKafkaInfo;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class TagProcessFunction extends ProcessWindowFunction<TagKafkaInfo, Map<String, Set<String>>, String, TimeWindow> {

    private MapState<String, Set<String>> mapState;

    @Override
    public void open(Configuration parameters) throws Exception {
        mapState = getRuntimeContext().getMapState(
                new MapStateDescriptor<String, Set<String>>(
                        "mapstate",
                        Types.STRING,
                        TypeInformation.of(new TypeHint<Set<String>>() {
                        }))
        );
    }

    @Override
    public void process(String key, ProcessWindowFunction<TagKafkaInfo, Map<String, Set<String>>, String, TimeWindow>.Context context, Iterable<TagKafkaInfo> elements, Collector<Map<String, Set<String>>> out) throws Exception {
        HashSet<String> set = new HashSet<>();
        for (TagKafkaInfo element : elements) {
            set.add(element.getName());
        }
        if (!mapState.contains(key)) {
            mapState.put(key, set);
        } else {
            mapState.get(key).addAll(set);
        }
        HashMap<String, Set<String>> stringSetHashMap = new HashMap<>();
        stringSetHashMap.put(key, mapState.get(key));
        out.collect(stringSetHashMap);
        mapState.remove(key);
        set.clear();
    }
}
