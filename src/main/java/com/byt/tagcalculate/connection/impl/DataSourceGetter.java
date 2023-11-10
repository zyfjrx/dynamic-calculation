package com.byt.tagcalculate.connection.impl;

import com.alibaba.druid.pool.DruidDataSource;
import com.byt.common.utils.ConfigManager;

public class DataSourceGetter {
    public static DruidDataSource getMysqlDataSource() {
        String url = "jdbc:mysql://"+ConfigManager.getProperty("mysql.host")+":"+ConfigManager.getProperty("mysql.port")+"/"+ConfigManager.getProperty("mysql.database")+"?autoReconnect=true&useSSL=false&characterEncoding=utf-8&serverTimezone=GMT%2B8";
        DruidDataSource druidDataSource = new DruidDataSource();
        druidDataSource.setUrl(url);
        druidDataSource.setUsername(ConfigManager.getProperty("mysql.username"));
        druidDataSource.setPassword(ConfigManager.getProperty("mysql.password"));
        druidDataSource.setDefaultAutoCommit(false);
        druidDataSource.setMaxActive(10);
        druidDataSource.setInitialSize(4);
        druidDataSource.setMinIdle(5);
        return druidDataSource;
    }
    public static DruidDataSource getGpDataSource() {
        DruidDataSource druidDataSource = new DruidDataSource();
        druidDataSource.setUrl(ConfigManager.getProperty("greenplum.url"));
        druidDataSource.setUsername(ConfigManager.getProperty("greenplum.user"));
        druidDataSource.setPassword(ConfigManager.getProperty("greenplum.pass"));
        druidDataSource.setDefaultAutoCommit(false);
        druidDataSource.setMaxActive(10);
        druidDataSource.setInitialSize(4);
        druidDataSource.setMinIdle(5);
        return druidDataSource;
    }

    public static DruidDataSource getMysqlDataSource(String host,Integer port,String username,String password,String database) {
        String url = "jdbc:mysql://"+host+":"+port+"/"+database+"?autoReconnect=true&useSSL=false&characterEncoding=utf-8&serverTimezone=GMT%2B8";
        System.out.println(url);
        DruidDataSource druidDataSource = new DruidDataSource();
        druidDataSource.setUrl(url);
        druidDataSource.setUsername(username);
        druidDataSource.setPassword(password);
        druidDataSource.setDefaultAutoCommit(false);
        // 高峰期 看数据库能承担的最大并发数
        druidDataSource.setMaxActive(20);
        // 初始连接 日常并发
        druidDataSource.setInitialSize(5);
        // 空闲时候 最少保持连接数
        druidDataSource.setMinIdle(5);
        druidDataSource.setMaxWait(10000L);
        // 借出连接时测试 保证使用时连接是好的
        //druidDataSource.setTestOnBorrow(true);
        // 空闲时周期测试 压力小不能保证借走的时候是好的
        //druidDataSource.setTestWhileIdle(true);
        // 归还连接时测试
        //druidDataSource.setTestOnReturn(false);
        druidDataSource.setTransactionQueryTimeout(60);
        return druidDataSource;
    }
}
