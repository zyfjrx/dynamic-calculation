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
        return druidDataSource;
    }
    public static DruidDataSource getGpDataSource() {
        DruidDataSource druidDataSource = new DruidDataSource();
        druidDataSource.setUrl(ConfigManager.getProperty("greenplum.url"));
        druidDataSource.setUsername(ConfigManager.getProperty("greenplum.user"));
        druidDataSource.setPassword(ConfigManager.getProperty("greenplum.pass"));
        druidDataSource.setDefaultAutoCommit(false);
        return druidDataSource;
    }
}
