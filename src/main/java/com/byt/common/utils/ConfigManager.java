package com.byt.common.utils;

import com.byt.tagcalculate.constants.PropertiesConstants;
import org.apache.commons.lang3.math.NumberUtils;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;


/**
 * @title: 配置参数处理类
 * @author: zhangyifan
 * @date: 2022/6/29 10:03
 */
public class ConfigManager {

    private static Properties prop = new Properties();

    static {
        try {
            Path path = Paths.get("./application.properties");
            // 如果jar包当前路径有配置文件，则加载当前路径下
            if (Files.exists(path)){
                InputStream outProp = Files.newInputStream(path);
                prop.load(new InputStreamReader(outProp,"UTF-8"));
            }else {
                InputStream innerProp = ConfigManager.class.getClassLoader().getResourceAsStream("application.properties");
                prop.load(new InputStreamReader(innerProp, "UTF-8"));
            }
        } catch (Exception e) {
            System.err.println(e);
        }
    }

    /**
     * 获取指定key对应的value
     *
     * @param key
     * @return value
     */
    public static String getProperty(String key) {
        return prop.getProperty(key);
    }

    public static List<String> getListProperty(String key) {
        System.out.println("正在读取topic"+Arrays.asList(prop.getProperty(key).split(",")));
        return Arrays.asList(prop.getProperty(key).split(","));
    }

    /**
     * 获取整数类型的配置项
     */
    public static Integer getInteger(String key) {
        return NumberUtils.toInt(getProperty(key));
    }

    /**
     * 获取布尔类型的配置项
     *
     * @param key
     * @return value
     */
    public static Boolean getBoolean(String key) {
        String value = getProperty(key);
        try {
            return Boolean.valueOf(value);
        } catch (Exception e) {
            System.err.println(e);
        }
        return false;
    }

    /**
     * 获取Long类型的配置项
     *
     * @param key
     * @return
     */
    public static Long getLong(String key) {
        return NumberUtils.toLong(getProperty(key));
    }



    public static void main(String[] args) {
        System.out.println(ConfigManager.getListProperty(PropertiesConstants.KAFKA_ODS_TOPIC));
    }
}