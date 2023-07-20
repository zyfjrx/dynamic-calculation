package com.byt.tagwarning.pojo;

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
    public String tag_name;
    public String tag_topic;
    public String send_period; // 发送周期
    public Integer send_times; // 发送次数
}
