package com.byt.common.utils;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @title: 线程池工具类(异步io使用)
 * @author: zhang
 * @date: 2023/7/20 20:31
 * 线程安全(懒汉式)
 */
public class ThreadPoolUtil {

    private static ThreadPoolExecutor threadPool;

    /*
        corePoolSize:初始线程数量
        maximumPoolSiz：最大连接数
        keepAliveTime：当空闲线程超过corePoolSize时。多余的线程会在多长时间销毁
     */
    public static ThreadPoolExecutor getThreadPoolExecutor() {
        if (threadPool == null) {
            synchronized (ThreadPoolUtil.class) {
                if (threadPool == null) {
                    System.out.println("--------创建线程池对象-------");
                    threadPool = new ThreadPoolExecutor(
                            6,12,300L,
                            TimeUnit.SECONDS,new LinkedBlockingDeque<Runnable>(Integer.MAX_VALUE)
                    );
                }
            }
        }
        return threadPool;
    }
}
