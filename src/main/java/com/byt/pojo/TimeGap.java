package com.byt.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @title:
 * @author: zhangyifan
 * @date: 2022/7/1 16:17
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TimeGap implements Serializable {
    private String timeGap ;
    private String slideGap ;
}
