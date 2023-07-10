package com.byt.tagcalculate.calculate.calculatechain;

import com.byt.tagcalculate.constants.PropertiesConstants;
import com.byt.tagcalculate.pojo.TagKafkaInfo;
import com.byt.common.utils.ConfigManager;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @title: 时间窗口选择器
 * @author: zhangyifan
 * @date: 2022/8/29 10:10
 */
public class WindowTimeSelector {

    public static WindowAssigner<? super TagKafkaInfo, TimeWindow> getWindowTime(Time size, Time slide){
        if (ConfigManager.getProperty(PropertiesConstants.STREAM_TIME_WINDOW).equals("process")){
            return SlidingProcessingTimeWindows.of(size,slide);
        }else {
            return SlidingEventTimeWindows.of(size,slide);
        }
    }
}
