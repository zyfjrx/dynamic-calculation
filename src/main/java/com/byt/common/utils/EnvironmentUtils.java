package com.byt.common.utils;

import com.byt.tagcalculate.constants.PropertiesConstants;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @title: flink读取配置工具类
 * @author: zhangyifan
 * @date: 2022/6/21 13:41
 */
public class EnvironmentUtils {
    public static ParameterTool createParameterTool() throws IOException {
        // 获取jar中默认配置
        ParameterTool defaultPropertiesFile = ParameterTool.fromPropertiesFile(
                EnvironmentUtils.class.getClassLoader().getResourceAsStream("application.properties")
        );

        // 获取外部配置
        URL url = Test.class.getProtectionDomain().getCodeSource().getLocation();
        Pattern p = Pattern.compile(".*\\.properties");
        ParameterTool externalPropertiesFile = null;
        ParameterTool parameterTool = null;
        try {
            String filePath = URLDecoder.decode(url.getPath(), "utf-8");// 转化为utf-8编码，支持中文
            if (filePath.endsWith(".jar")) {
                filePath = filePath.substring(0, filePath.lastIndexOf("/") + 1);
            }
            File folder = new File(filePath);
            File[] files = folder.listFiles();
            if (files == null || files.length == 0) {
                System.out.println("目标文件夹中没有任何文件");
                return defaultPropertiesFile;
            }
            for (File file : files) {
                if (!file.isFile()) {
                    continue;
                }
                Matcher matcher = p.matcher(file.getName());
                if (matcher.matches()) {
                    externalPropertiesFile = ParameterTool.fromPropertiesFile(file);
                }
            }

            parameterTool = defaultPropertiesFile.mergeWith(externalPropertiesFile);
            return parameterTool;
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("外部配置文件异常");
            return defaultPropertiesFile;
        }
    }


    public static StreamExecutionEnvironment prepareEnv(ParameterTool parameterTool) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(parameterTool.getInt(PropertiesConstants.STREAM_PARALLELISM, 1));


        // ck设置
        if (parameterTool.getBoolean(PropertiesConstants.STREAM_CHECKPOINT_ENABLE, false)) {

            env.enableCheckpointing(parameterTool.getLong(PropertiesConstants.STREAM_CHECKPOINT_INTERVAL, 10000L));

            // 设置故障重启策略
            env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 60000L));

            // 对 FsStateBackend 刷出去的文件进行文件压缩，减小 checkpoint 体积
            env.getConfig().setUseSnapshotCompression(true);
        }
        // 全局配置
        env.getConfig().setGlobalJobParameters(parameterTool);

        return env;
    }

    public static StreamExecutionEnvironment prepareWithWebUIEnv(ParameterTool parameterTool) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(parameterTool.getConfiguration());
        // 设置并行度
        env.setParallelism(parameterTool.getInt(PropertiesConstants.STREAM_PARALLELISM, 5));
        // 设置故障重启策略
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 60000L));
        // ck设置
        if (parameterTool.getBoolean(PropertiesConstants.STREAM_CHECKPOINT_ENABLE, true)) {
            env.enableCheckpointing(parameterTool.getLong(PropertiesConstants.STREAM_CHECKPOINT_INTERVAL, 10000L));
        }
        // 全局配置
        env.getConfig().setGlobalJobParameters(parameterTool);
        // 对 FsStateBackend 刷出去的文件进行文件压缩，减小 checkpoint 体积
        env.getConfig().setUseSnapshotCompression(true);

        return env;
    }
}
