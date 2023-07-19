package com.byt.tagcalculate.calculate.dynamicwindow;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
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
public class DynamicSlidingProcessingTimeWindows<T> extends WindowAssigner<T, TimeWindow> {
    private static final long serialVersionUID = 1L;
    private final long size;
    private final long offset;
    private final long slide;


    //从原始数据中获取窗口长度
    private final TimeAdjustExtractor<T> sizeTimeAdjustExtractor;
    //从原始数据中获取窗口步长
    private final TimeAdjustExtractor<T> slideTimeAdjustExtractor;

    private DynamicSlidingProcessingTimeWindows(long size, long slide, long offset) {
        if (Math.abs(offset) < slide && size > 0L) {
            this.size = size;
            this.slide = slide;
            this.offset = offset;
            this.sizeTimeAdjustExtractor = (ele) -> 0;
            this.slideTimeAdjustExtractor = (ele) -> 0;
        } else {
            throw new IllegalArgumentException("DynamicSlidingProcessingTimeWindows parameters must satisfy abs(offset) < slide and size > 0");
        }
    }

    public DynamicSlidingProcessingTimeWindows(long size, long offset, long slide, TimeAdjustExtractor<T> sizeTimeAdjustExtractor, TimeAdjustExtractor<T> slideTimeAdjustExtractor) {
        if (Math.abs(offset) >= slide || size <= 0) {
            throw new IllegalArgumentException(
                    "DynamicSlidingProcessingTimeWindows parameters must satisfy "
                            + "abs(offset) < slide and size > 0");
        }
        this.size = size;
        this.offset = offset;
        this.slide = slide;
        this.sizeTimeAdjustExtractor = sizeTimeAdjustExtractor;
        this.slideTimeAdjustExtractor = slideTimeAdjustExtractor;
    }

    @Override
    public Collection<TimeWindow> assignWindows(T element, long timestamp, WindowAssignerContext context) {
        timestamp = context.getCurrentProcessingTime();
        long realSize = this.sizeTimeAdjustExtractor.extract(element);
        long realSlide = this.slideTimeAdjustExtractor.extract(element);
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
        return "DynamicSlidingProcessingTimeWindows(" + this.size + ", " + this.slide + ")";
    }

    public static DynamicSlidingProcessingTimeWindows of(Time size, Time slide) {
        return new DynamicSlidingProcessingTimeWindows(size.toMilliseconds(), slide.toMilliseconds(), 0L);
    }



    public static DynamicSlidingProcessingTimeWindows of(Time size, Time slide, Time offset) {
        return new DynamicSlidingProcessingTimeWindows(size.toMilliseconds(), slide.toMilliseconds(), offset.toMilliseconds());
    }

    public static <T> DynamicSlidingProcessingTimeWindows<T> of(TimeAdjustExtractor<T> sizeTimeAdjustExtractor, TimeAdjustExtractor<T> slideTimeAdjustExtractor) {
        return new DynamicSlidingProcessingTimeWindows(5 * 1000L, 5 * 1000L, 0L,sizeTimeAdjustExtractor,slideTimeAdjustExtractor);
    }

    public static DynamicSlidingProcessingTimeWindows of(Time size, Time slide, Time offset, TimeAdjustExtractor sizeTimeAdjustExtractor, TimeAdjustExtractor slideTimeAdjustExtractor) {
        return new DynamicSlidingProcessingTimeWindows(
                size.toMilliseconds(), slide.toMilliseconds(), offset.toMilliseconds(),
                sizeTimeAdjustExtractor,slideTimeAdjustExtractor);
    }

    public static DynamicSlidingProcessingTimeWindows of(Time size, Time slide, TimeAdjustExtractor sizeTimeAdjustExtractor, TimeAdjustExtractor slideTimeAdjustExtractor) {
        return new DynamicSlidingProcessingTimeWindows(
                size.toMilliseconds(), slide.toMilliseconds(), 0,
                sizeTimeAdjustExtractor,slideTimeAdjustExtractor);
    }


    @Override
    public Trigger<T, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
        return DynamicProcessingTimeTrigger.<T>create();
    }

    @Override
    public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
        return new TimeWindow.Serializer();
    }

    @Override
    public boolean isEventTime() {
        return false;
    }
}
