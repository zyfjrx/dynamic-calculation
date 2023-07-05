package com.byt.utils;

import com.byt.constants.PropertiesConstants;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

/**
 * @title: flink读取配置工具类
 * @author: zhangyifan
 * @date: 2022/6/21 13:41
 */
public class EnvironmentUtils {


    public static ParameterTool createParameterTool(String[] args) throws IOException {
        // 系统环境参数 -Dyy=xx
        ParameterTool systemProperties = ParameterTool.fromSystemProperties();

        // 运行参数 main参数  flink自己数据需要 - 或者 -- 开头做key 例如 -name tom or --name tom
        ParameterTool fromArgs = ParameterTool.fromArgs(args);

        // 默认配置文件
        ParameterTool defaultPropertiesFile = ParameterTool.fromPropertiesFile(
                EnvironmentUtils.class.getResourceAsStream(PropertiesConstants.FLINK_DEV_PROPERTIES)
        );

        // 合并参数
        ParameterTool parameterTool = defaultPropertiesFile
                .mergeWith(fromArgs)
                .mergeWith(systemProperties);
        return parameterTool;
    }


    public static StreamExecutionEnvironment prepareEnv(ParameterTool parameterTool) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(parameterTool.getInt(PropertiesConstants.STREAM_PARALLELISM,1));


        // ck设置
        if (parameterTool.getBoolean(PropertiesConstants.STREAM_CHECKPOINT_ENABLE,false)){

            env.enableCheckpointing(parameterTool.getLong(PropertiesConstants.STREAM_CHECKPOINT_INTERVAL,10000L));

            // 设置故障重启策略
            env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4,60000L));

            // 对 FsStateBackend 刷出去的文件进行文件压缩，减小 checkpoint 体积
            env.getConfig().setUseSnapshotCompression(true);
        }
        // 全局配置
        env.getConfig().setGlobalJobParameters(parameterTool);

        return env;
    }

    public static StreamExecutionEnvironment prepareWithWebUIEnv(ParameterTool parameterTool) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(parameterTool.getConfiguration());
        // 设置并行度
        env.setParallelism(parameterTool.getInt(PropertiesConstants.STREAM_PARALLELISM,5));
        // 设置故障重启策略
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4,60000L));
        // ck设置
        if (parameterTool.getBoolean(PropertiesConstants.STREAM_CHECKPOINT_ENABLE,true)){
            env.enableCheckpointing(parameterTool.getLong(PropertiesConstants.STREAM_CHECKPOINT_INTERVAL,10000L));
        }
        // 全局配置
        env.getConfig().setGlobalJobParameters(parameterTool);
        // 对 FsStateBackend 刷出去的文件进行文件压缩，减小 checkpoint 体积
        env.getConfig().setUseSnapshotCompression(true);

        return env;
    }
}
