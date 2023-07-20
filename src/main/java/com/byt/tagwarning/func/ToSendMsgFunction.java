package com.byt.tagwarning.func;

/**
 * @title:
 * @author: zhangyf
 * @date: 2023/6/13 9:49
 **/
public interface ToSendMsgFunction<T> {
    String getMsg(T input);
}
