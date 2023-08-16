package com.byt.common.utils;

import org.apache.commons.lang3.math.NumberUtils;

import java.io.*;
import java.net.URL;
import java.net.URLDecoder;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * @title: 配置参数处理类
 * @author: zhangyifan
 * @date: 2022/6/29 10:03
 */
public class ConfigManager {

    private static Properties prop = new Properties();

//    public static Properties getProp() throws IOException {
//        Properties defaultProp = new Properties();
//        Properties externalProp = new Properties();
//        InputStream innerProp = ConfigManager.class.getClassLoader().getResourceAsStream("application.properties");
//        defaultProp.load(new InputStreamReader(innerProp, "UTF-8"));
//
//        URL url = EnvironmentUtils.class.getProtectionDomain().getCodeSource().getLocation();
//        Pattern p = Pattern.compile(".*\\.properties");
//        String filePath = URLDecoder.decode(url.getPath(), "utf-8");// 转化为utf-8编码，支持中文
//        try {
//            if (filePath.endsWith(".jar")) {
//                filePath = filePath.substring(0, filePath.lastIndexOf("/") + 1);
//            }
//            System.out.println(filePath);
//            File folder = new File(filePath);
//            File[] files = folder.listFiles();
//            if (files == null || files.length == 0) {
//                System.out.println("目标文件夹中没有任何文件");
//                return defaultProp;
//            }
//
//            for (File file : files) {
//                if (!file.isFile()) {
//                    continue;
//                }
//                Matcher matcher = p.matcher(file.getName());
//                if (matcher.matches()) {
//                    FileInputStream fis = new FileInputStream(file);
//                    externalProp.load(new InputStreamReader(fis, "UTF-8"));
//                }
//            }
//            Map<String,String> defaultMap = (Map) defaultProp;
//            Set<String> defaultKey = defaultMap.keySet();
//            for (String key : defaultKey) {
//                if (externalProp.containsKey(key)){
//                    defaultProp.setProperty(key, externalProp.getProperty(key));
//                }
//            }
//            return defaultProp;
//        } catch (Exception e) {
//            e.printStackTrace();
//            return defaultProp;
//        }
//    }

    static {
        try {
            URL url = EnvironmentUtils.class.getProtectionDomain().getCodeSource().getLocation();
            Pattern p = Pattern.compile(".*\\.properties");
            String filePath = URLDecoder.decode(url.getPath(), "utf-8");// 转化为utf-8编码，支持中文
            if (filePath.endsWith(".jar")) {
                filePath = filePath.substring(0, filePath.lastIndexOf("/") + 1);
            }
            System.out.println(filePath);
            File folder = new File(filePath);
            File[] files = folder.listFiles();
            if (files == null || files.length == 0) {
                System.out.println("目标文件夹中没有任何文件");
                InputStream innerProp = ConfigManager.class.getClassLoader().getResourceAsStream("application.properties");
                prop.load(new InputStreamReader(innerProp, "UTF-8"));
            }
            for (File file : files) {
                if (!file.isFile()) {
                    continue;
                }
                Matcher matcher = p.matcher(file.getName());
                if (matcher.matches()) {
                    FileInputStream fis = new FileInputStream(file);
                    prop.load(new InputStreamReader(fis, "UTF-8"));
                }
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
        System.out.println("正在读取topic" + Arrays.asList(prop.getProperty(key).split(",")));
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
        System.out.println(ConfigManager.getProperty("mysql.host"));
        System.out.println(ConfigManager.class.getProtectionDomain().getCodeSource().getLocation());
        System.out.println(FormulaTag.START);
    }
}