package com.byt.tagcalculate.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Objects;

/**
 * @title: mysql配置信息pojo类
 * @author: zhang
 * @date: 2022/6/23 13:33
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TagProperties {
    public Integer id;
    public Integer line_id;
    public String tag_name;
    public String byt_name;
    public String tag_topic;
    public String tag_type;
    public String calculate_type;
    public String tag_desc;
    public String value_min;
    public String value_max;
    public String task_name;
    public String sink_table;
    public String param;
    public Integer status;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TagProperties that = (TagProperties) o;
        return Objects.equals(id, that.id) && Objects.equals(line_id, that.line_id) && Objects.equals(tag_name, that.tag_name) && Objects.equals(byt_name, that.byt_name) && Objects.equals(tag_topic, that.tag_topic) && Objects.equals(tag_type, that.tag_type) && Objects.equals(calculate_type, that.calculate_type) && Objects.equals(tag_desc, that.tag_desc) && Objects.equals(value_min, that.value_min) && Objects.equals(value_max, that.value_max) && Objects.equals(task_name, that.task_name) && Objects.equals(param, that.param) && Objects.equals(status, that.status);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, line_id, tag_name, byt_name, tag_topic, tag_type, calculate_type, tag_desc, value_min, value_max, task_name, param, status);
    }
}
