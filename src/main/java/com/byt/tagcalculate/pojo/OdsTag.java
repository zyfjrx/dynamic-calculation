package com.byt.tagcalculate.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * @title:
 * @author: zhangyf
 * @date: 2024/3/20 10:36
 **/
@Data
@AllArgsConstructor
@NoArgsConstructor
public class OdsTag {
    private String tagName;
    private String time;
    private String topic;
    private String strValue;
    private BigDecimal value;
}
