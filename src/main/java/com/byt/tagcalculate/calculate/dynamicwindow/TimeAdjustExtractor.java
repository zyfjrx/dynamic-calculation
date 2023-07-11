package com.byt.tagcalculate.calculate.dynamicwindow;

import java.io.Serializable;

/**
 * @title: 提取窗口时间接口
 * @author: zhangyf
 * @date: 2023/7/11 14:37
 **/
public interface TimeAdjustExtractor<T> extends Serializable {
    long extract(T element);
}
