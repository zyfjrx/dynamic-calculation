package com.byt.tagcalculate.calculate;

import com.byt.tagcalculate.pojo.TagKafkaInfo;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
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
public class DynamicSlidingEventTimeWindows extends WindowAssigner<Object, TimeWindow> {
    private static final long serialVersionUID = 1L;
    private final long size;
    private final long offset;
    private final long slide;

    private DynamicSlidingEventTimeWindows(long size, long slide, long offset) {
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
        TagKafkaInfo tagKafkaInfo = null;
        try {
            tagKafkaInfo = (TagKafkaInfo) element;
        } catch (Exception e) {
            e.printStackTrace();
        }
        long realSize = timeParams(tagKafkaInfo.getWinSize());
        long realSlide = timeParams(tagKafkaInfo.getWinSlide());
        if (timestamp > Long.MIN_VALUE) {
            List<TimeWindow> windows = new ArrayList((int) ((realSize == 0 ? size : realSize) / (realSlide == 0 ? slide : realSlide)));
            long lastStart = TimeWindow.getWindowStartWithOffset(timestamp, this.offset, (realSlide == 0 ? slide : realSlide));

            for (long start = lastStart; start > timestamp - (realSize == 0 ? size : realSize); start -= (realSlide == 0 ? slide : realSlide)) {
                windows.add(new TimeWindow(start, start + (realSize == 0 ? size : realSize)));
            }
            return windows;
        } else {
            throw new RuntimeException(
                    "Record has Long.MIN_VALUE timestamp (= no timestamp marker). "
                            + "Is the time characteristic set to 'ProcessingTime', or did you forget to call "
                            + "'DataStream.assignTimestampsAndWatermarks(...)'?");
        }
    }


    public long getSize() {
        return this.size;
    }

    public long getSlide() {
        return this.slide;
    }


    public String toString() {
        return "DynamicSlidingEventTimeWindows(" + this.size + ", " + this.slide + ")";
    }

    public static DynamicSlidingEventTimeWindows of(Time size, Time slide) {
        return new DynamicSlidingEventTimeWindows(size.toMilliseconds(), slide.toMilliseconds(), 0L);
    }

    public static DynamicSlidingEventTimeWindows of() {
        // 默认值 数据流中没有开窗参数时使用
        return new DynamicSlidingEventTimeWindows(5 * 1000L, 5 * 1000L, 0L);
    }

    public static DynamicSlidingEventTimeWindows of(Time size, Time slide, Time offset) {
        return new DynamicSlidingEventTimeWindows(size.toMilliseconds(), slide.toMilliseconds(), offset.toMilliseconds());
    }


    @Override
    public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
        return EventTimeTrigger.create();
    }

    @Override
    public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
        return new TimeWindow.Serializer();
    }

    @Override
    public boolean isEventTime() {
        return true;
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
