package com.byt.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

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
    public String param;
    public Integer status;
}
