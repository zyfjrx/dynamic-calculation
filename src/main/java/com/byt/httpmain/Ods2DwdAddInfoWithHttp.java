package com.byt.httpmain;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.byt.constants.PropertiesConstants;
import com.byt.utils.ConfigManager;
import com.byt.utils.HttpUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

/**
 * @title: 通过http方式获取配置
 * @author: zhangyifan
 * @date: 2022/7/21 14:31
 */
public class Ods2DwdAddInfoWithHttp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.1 开启检查点
        env.enableCheckpointing(ConfigManager.getLong(PropertiesConstants.STREAM_CHECKPOINT_INTERVAL), CheckpointingMode.EXACTLY_ONCE);
        //2.2 设置检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(ConfigManager.getLong(PropertiesConstants.STREAM_CHECKPOINT_TIMEOUT));
        //2.3 设置取消job后，检查点是否保留
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //2.4 设置重启策略
        //固定次数重启
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000L));
        //失败率重启
        //env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.milliseconds(3000), Time.days(30)));
        //2.5 设置检查点间隔时间
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(ConfigManager.getLong(PropertiesConstants.STREAM_CHECKPOINT_MIN_PAUSE_BETWEEN_CHECKPOINTS));
        //2.6 设置状态后段
        env.setStateBackend(new FsStateBackend("hdfs://kafka1:9000/flink/ck/httpck"));
        //2.7 设置操作hadoop用户
        System.setProperty("HADOOP_USER_NAME", "root");

        env
                .addSource(new RichSourceFunction<String>() {
                    private boolean running = true;

                    @Override
                    public void run(SourceContext<String> sourceContext) throws Exception {
                        while (running) {
                            String s = HttpUtil.doGet("http://192.168.2.179:8080/paras");
                            sourceContext.collect(s);
                            Thread.sleep(10000L);
                        }
                    }

                    @Override
                    public void cancel() {
                        running = false;
                    }
                })
                .map(new MapFunction<String, JSONArray>() {
                    @Override
                    public JSONArray map(String s) throws Exception {
                        return JSON.parseArray(s);
                    }
                })
                .print();


        env.execute();
    }
}
