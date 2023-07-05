package com.byt.pojo;

import com.byt.utils.TimeUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.Objects;

/**
 * @title: kafka数据封装pojo class
 * @author: zhang
 * @date: 2022/6/22 19:19
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TagKafkaInfo {
    private String name;
    private String time;
    private BigDecimal value;
    private String topic;
    private Integer tagType;
    private String bytName;
    private String strValue;
    private Long timestamp;
    private Integer isNormal;
    private String calculateType;
    private String calculateParam;
    private String taskName;
    private Integer lineId;
    private String winSize;
    private String winSlide;
    private Integer n;
    private Double a;// FOF
    private Long lowerInt; // DEJUMP
    private Long upperInt; // DEJUMP
    private Double dt; // KF
    private Double R; // KF
    private Integer totalIndex; // 算子链总长度
    private Integer currIndex; // 当前计算位置
    private String currCal; // 当前计算类型

    public Long getTimestamp() {
        if (this.timestamp == null && this.time != null) {
            timestamp = TimeUtil.getStartTime(this.time);
        }
        return timestamp;
    }
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TagKafkaInfo that = (TagKafkaInfo) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(time, that.time) &&
                Objects.equals(value, that.value) &&
                Objects.equals(topic, that.topic) &&
                Objects.equals(bytName, that.bytName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, time, value, topic, bytName);
    }

    @Override
    public String toString() {
        return "TagKafkaInfo{" +
                "name='" + name + '\'' +
                ", time='" + time + '\'' +
                ", value=" + value +
                ", topic='" + topic + '\'' +
                ", tagType=" + tagType +
                ", bytName='" + bytName + '\'' +
                ", strValue='" + strValue + '\'' +
                ", timestamp=" + timestamp +
                ", isNormal=" + isNormal +
                ", calculateType='" + calculateType + '\'' +
                ", calculateParam='" + calculateParam + '\'' +
                ", taskName='" + taskName + '\'' +
                ", lineId=" + lineId +
                ", winSize='" + winSize + '\'' +
                ", winSlide='" + winSlide + '\'' +
                ", n=" + n +
                ", a=" + a +
                ", lowerInt=" + lowerInt +
                ", upperInt=" + upperInt +
                ", dt=" + dt +
                ", R=" + R +
                ", totalIndex=" + totalIndex +
                ", currIndex=" + currIndex +
                ", currCal='" + currCal + '\'' +
                '}';
    }
}
