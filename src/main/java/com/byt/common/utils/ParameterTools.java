package com.byt.common.utils;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;

/**
 * @title: 获取配置参数
 * @author: zhangyf
 * @date: 2023/4/7 14:14
 **/
public class ParameterTools {
    public static ParameterTool createParameterTool() throws IOException {
        try {
            return ParameterTool
                    .fromPropertiesFile(ParameterTools.class.getResourceAsStream("/application.properties"))
                    .mergeWith(ParameterTool.fromPropertiesFile("/opt/jupiterApp/flink-project/application.properties"));

        }catch (IOException e){
            e.printStackTrace();
        }
        return ParameterTool
                .fromPropertiesFile(ParameterTools.class.getResourceAsStream("/application.properties"));
    }

    public static void main(String[] args) throws IOException {
//        System.out.println(ParameterTool
//                .fromPropertiesFile(ParameterTools.class.getResourceAsStream("/application.properties"))
//                .mergeWith(ParameterTool.fromPropertiesFile("D:\\byt\\flink-realtime\\protoc\\application.properties"))
//                .get("kafka.ods.topic"));

        System.out.println(ParameterTools.createParameterTool().get("mysql.host"));
    }
}
