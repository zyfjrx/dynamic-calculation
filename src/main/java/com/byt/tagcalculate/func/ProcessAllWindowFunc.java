package com.byt.tagcalculate.func;

import com.byt.tagcalculate.pojo.Event;
import com.google.common.collect.Lists;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * @title:
 * @author: zhangyf
 * @date: 2024/3/22 14:53
 **/
public class ProcessAllWindowFunc extends ProcessAllWindowFunction<Event, List<Event>, TimeWindow> {
    private List<Event> events;
    @Override
    public void open(Configuration parameters) throws Exception {
        events = new ArrayList<>();
    }

    @Override
    public void process(ProcessAllWindowFunction<Event, List<Event>, TimeWindow>.Context context, Iterable<Event> iterable, Collector<List<Event>> collector) throws Exception {
        events = Lists.newArrayList(iterable.iterator());
        collector.collect(events);
        events.clear();
    }
}
