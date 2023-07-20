package com.byt.tagwarning.func;


import com.byt.common.utils.HttpUtil;
import com.byt.common.utils.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.concurrent.ExecutorService;

/**
 * @title: 异步发送消息
 * @author: zhangyf
 * @date: 2023/6/13 9:42
 **/
public abstract class SendMsgAsyncFunction<T> extends RichAsyncFunction<T, T> implements ToSendMsgFunction<T> {
    private ExecutorService executorService;

    @Override
    public void open(Configuration parameters) throws Exception {
        executorService = ThreadPoolUtil.getThreadPoolExecutor();
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        // 开启多线程发送邮件
        executorService.submit(
                new Runnable() {
                    @Override
                    public void run() {
                        try {
                            String msg = getMsg(input);
                            HttpUtil.doPostString("http://localhost:8080/httpstr", msg);
                        } catch (Exception e) {
                            e.printStackTrace();
                            throw new RuntimeException("企业微信告警异常～～～");
                        }
                    }
                }

        );
    }

    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        super.timeout(input, resultFuture);
    }
}
