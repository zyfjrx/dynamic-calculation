package com.byt.common.utils;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @title:
 * @author: zhangyf
 * @date: 2023/8/14 14:04
 **/
public class EnvironmentUtils {
    public static ParameterTool createParameterTool() throws IOException {
        // 获取jar中默认配置
        ParameterTool defaultPropertiesFile = ParameterTool.fromPropertiesFile(
                EnvironmentUtils.class.getClassLoader().getResourceAsStream("application.properties")
        );

        // 获取外部配置
        URL url = EnvironmentUtils.class.getProtectionDomain().getCodeSource().getLocation();
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

    public static void main(String[] args) throws IOException {
        System.out.println(createParameterTool().get("kafka.server"));
    }
}
