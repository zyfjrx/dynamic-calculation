package com.byt.tagcalculate.pojo;

import com.byt.common.utils.TimeUtil;
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
    private Long winSize;
    private Long winSlide;
    private Integer currNBefore; //1
    private boolean changeNFA = false; // 0 初始化 oldNBefore=currNBefore
    private String op; // 操作类型
    private Double a;// FOF
    private Double lowerInt; // DEJUMP
    private Double upperInt; // DEJUMP
    private Double dt; // KF
    private Double R; // KF
    private Integer totalIndex; // 算子链总长度
    private Integer currIndex; // 当前计算位置
    private String currCal; // 当前计算类型
    private Integer status; // 当前计算类型

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

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public BigDecimal getValue() {
        return value;
    }

    public void setValue(BigDecimal value) {
        this.value = value;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Integer getTagType() {
        return tagType;
    }

    public void setTagType(Integer tagType) {
        this.tagType = tagType;
    }

    public String getBytName() {
        return bytName;
    }

    public void setBytName(String bytName) {
        this.bytName = bytName;
    }

    public String getStrValue() {
        return strValue;
    }

    public void setStrValue(String strValue) {
        this.strValue = strValue;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Integer getIsNormal() {
        return isNormal;
    }

    public void setIsNormal(Integer isNormal) {
        this.isNormal = isNormal;
    }

    public String getCalculateType() {
        return calculateType;
    }

    public void setCalculateType(String calculateType) {
        this.calculateType = calculateType;
    }

    public String getCalculateParam() {
        return calculateParam;
    }

    public void setCalculateParam(String calculateParam) {
        this.calculateParam = calculateParam;
    }

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public Integer getLineId() {
        return lineId;
    }

    public void setLineId(Integer lineId) {
        this.lineId = lineId;
    }

    public Long getWinSize() {
        return winSize;
    }

    public void setWinSize(Long winSize) {
        this.winSize = winSize;
    }

    public Long getWinSlide() {
        return winSlide;
    }

    public void setWinSlide(Long winSlide) {
        this.winSlide = winSlide;
    }


    public Double getA() {
        return a;
    }

    public void setA(Double a) {
        this.a = a;
    }

    public Double getLowerInt() {
        return lowerInt;
    }

    public void setLowerInt(Double lowerInt) {
        this.lowerInt = lowerInt;
    }

    public Double getUpperInt() {
        return upperInt;
    }

    public void setUpperInt(Double upperInt) {
        this.upperInt = upperInt;
    }

    public Double getDt() {
        return dt;
    }

    public void setDt(Double dt) {
        this.dt = dt;
    }

    public Double getR() {
        return R;
    }

    public void setR(Double r) {
        R = r;
    }

    public Integer getTotalIndex() {
        return totalIndex;
    }

    public void setTotalIndex(Integer totalIndex) {
        this.totalIndex = totalIndex;
    }

    public Integer getCurrIndex() {
        return currIndex;
    }

    public void setCurrIndex(Integer currIndex) {
        this.currIndex = currIndex;
    }

    public String getCurrCal() {
        return currCal;
    }

    public void setCurrCal(String currCal) {
        this.currCal = currCal;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public Integer getCurrNBefore() {
        return currNBefore;
    }

    public void setCurrNBefore(Integer currNBefore) {
        this.currNBefore = currNBefore;
    }

    public boolean getChangeNFA() {
        return changeNFA;
    }

    public void setChangeNFA(boolean changeNFA) {
        this.changeNFA = changeNFA;
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
                ", winSize=" + winSize +
                ", winSlide=" + winSlide +
                ", currNBefore=" + currNBefore +
                ", changeNFA=" + changeNFA +
                ", a=" + a +
                ", lowerInt=" + lowerInt +
                ", upperInt=" + upperInt +
                ", dt=" + dt +
                ", R=" + R +
                ", totalIndex=" + totalIndex +
                ", currIndex=" + currIndex +
                ", currCal='" + currCal + '\'' +
                ", status=" + status +
                '}';
    }
}
