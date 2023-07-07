package com.byt.tagcalculate.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.BroadcastState;

/**
 * @title:
 * @author: zhangyifan
 * @date: 2022/8/13 16:21
 */
public interface CDCState<T> {
    void broadcastStateAdd(JSONObject jo, BroadcastState<String, T> broadcastState) throws Exception;

    void broadcastStateDel(JSONObject jo, BroadcastState<String, T> broadcastState) throws Exception;

    default void opHandler(JSONObject value, BroadcastState<String, T> broadcastState) throws Exception {
        JSONObject before = value.getJSONObject("before");
        JSONObject after = value.getJSONObject("after");
        String op = value.getString("op");
        switch (op) {
            case "u":
                broadcastStateDel(before, broadcastState);
                broadcastStateAdd(after, broadcastState);
                break;
            case "d":
                broadcastStateDel(before, broadcastState);
                break;
            default:
                broadcastStateAdd(after, broadcastState);
        }
    }

}
