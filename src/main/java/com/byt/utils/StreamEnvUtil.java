package com.byt.utils;

import com.byt.constants.PropertiesConstants;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @title:
 * @author: zhangyifan
 * @date: 2022/7/11 13:24
 */
public class StreamEnvUtil {
    public static StreamExecutionEnvironment getEnv(String dir) {
        //TODO 1.获取执行环境信息
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(ConfigManager.getInteger(PropertiesConstants.STREAM_PARALLELISM));
        Boolean isEnable = ConfigManager.getBoolean(PropertiesConstants.STREAM_CHECKPOINT_ENABLE);

        CheckpointingMode checkpointingMode;
        if (ConfigManager.getInteger(PropertiesConstants.STREAM_CHECKPOINTING_MODE) == 1){
            checkpointingMode = CheckpointingMode.AT_LEAST_ONCE;
        }else {
            checkpointingMode = CheckpointingMode.EXACTLY_ONCE;
        }
            if (isEnable) {
                //2.1 开启检查点
                env.enableCheckpointing(ConfigManager.getLong(PropertiesConstants.STREAM_CHECKPOINT_INTERVAL), checkpointingMode);
                //2.2 设置检查点超时时间
                env.getCheckpointConfig().setCheckpointTimeout(ConfigManager.getLong(PropertiesConstants.STREAM_CHECKPOINT_TIMEOUT));
                //2.3 设置取消job后，检查点是否保留
                env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
                //设置可容忍的检查点失败数，默认值为0表示不允许容忍任何检查点失败
                env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
                //2.4 设置重启策略
                //固定次数重启
                env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 60000L));
                //失败率重启
                //env.setRestartStrategy(RestartStrategies.failureRateRestart(20, Time.milliseconds(6000), Time.days(1)));
                //2.5 设置检查点间隔时间
                env.getCheckpointConfig().setMinPauseBetweenCheckpoints(ConfigManager.getLong(PropertiesConstants.STREAM_CHECKPOINT_MIN_PAUSE_BETWEEN_CHECKPOINTS));
                //2.6 设置状态后段
                env.setStateBackend(new FsStateBackend(ConfigManager.getProperty(PropertiesConstants.STREAM_CHECKPOINT_DIR) + "/" + dir));
                //2.7 设置操作hadoop用户
                System.setProperty("HADOOP_USER_NAME", "root");
            }
        return env;
    }
}
