package com.byt.calculate;

import com.byt.pojo.TagKafkaInfo;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @title: Dynamic window
 * @author: zhangyf
 * @date: 2023/7/4 14:19
 **/
public class DynamicSlidingProcessingTimeWindows extends WindowAssigner<Object, TimeWindow> {
    private static final long serialVersionUID = 1L;
    private final long size;
    private final long offset;
    private final long slide;

    private DynamicSlidingProcessingTimeWindows(long size, long slide, long offset) {
        if (Math.abs(offset) < slide && size > 0L) {
            this.size = size;
            this.slide = slide;
            this.offset = offset;
        } else {
            throw new IllegalArgumentException("DynamicProcessingTimeWindows parameters must satisfy abs(offset) < slide and size > 0");
        }
    }

    @Override
    public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
        timestamp = context.getCurrentProcessingTime();
        TagKafkaInfo tagKafkaInfo = null;
        try {
            tagKafkaInfo = (TagKafkaInfo) element;
        } catch (Exception e) {
            e.printStackTrace();
        }
        long realSize = timeParams(tagKafkaInfo.getWinSize());
        long realSlide = timeParams(tagKafkaInfo.getWinSlide());
        List<TimeWindow> windows = new ArrayList((int) ((realSize == 0 ? size : realSize) / (realSlide == 0 ? slide : realSlide)));
        long lastStart = TimeWindow.getWindowStartWithOffset(timestamp, this.offset, (realSlide == 0 ? slide : realSlide));
        for (long start = lastStart; start > timestamp - (realSize == 0 ? size : realSize); start -= (realSlide == 0 ? slide : realSlide)) {
            windows.add(new TimeWindow(start, start + (realSize == 0 ? size : realSize)));
        }
        return windows;
    }


    public long getSize() {
        return this.size;
    }

    public long getSlide() {
        return this.slide;
    }


    public String toString() {
        return "DynamicProcessingTimeWindows(" + this.size + ", " + this.slide + ")";
    }

    public static DynamicSlidingProcessingTimeWindows of(Time size, Time slide) {
        return new DynamicSlidingProcessingTimeWindows(size.toMilliseconds(), slide.toMilliseconds(), 0L);
    }

    public static DynamicSlidingProcessingTimeWindows of() {
        // 默认值 数据流中没有开窗参数时使用
        return new DynamicSlidingProcessingTimeWindows(5 * 1000L, 5 * 1000L, 0L);
    }

    public static DynamicSlidingProcessingTimeWindows of(Time size, Time slide, Time offset) {
        return new DynamicSlidingProcessingTimeWindows(size.toMilliseconds(), slide.toMilliseconds(), offset.toMilliseconds());
    }


    @Override
    public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
        return ProcessingTimeTrigger.create();
    }

    @Override
    public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
        return new TimeWindow.Serializer();
    }

    @Override
    public boolean isEventTime() {
        return false;
    }

    public static Long timeParams(String str) {
        Long time = null;
        if (str.contains("s")) {
            time = Long.parseLong(str.replace("s", "")) * 1000L;
        } else if (str.contains("m")) {
            time = Long.parseLong(str.replace("m", "")) * 60L * 1000L;
        }
        return time;
    }
}
