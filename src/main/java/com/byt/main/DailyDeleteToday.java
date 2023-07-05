package com.byt.main;


import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.byt.connection.impl.DataSourceGetter;
import com.byt.utils.ConfigManager;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;


/**
 * @title: 数据清理
 * @author: zhangyifan
 * @date: 2022/9/18 16:51
 */

public class DailyDeleteToday {
    private final static String SELECT_SQL = "select 1";
    private static DruidPooledConnection pooledConnection;

    public static Connection getConnection() throws Exception{
        if (pooledConnection == null){
            DruidDataSource druidDataSource = null;
            druidDataSource = DataSourceGetter.getMysqlDataSource();
            try {
                assert druidDataSource != null;
                pooledConnection = druidDataSource.getConnection();
                pooledConnection.setAutoCommit(true);
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException("mysql" + " db connection failed");
            }
        }
        return pooledConnection.getConnection();
    }



    public static void main(String[] args) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat();// 格式化时间
        sdf.applyPattern("yyyy-MM-dd HH:mm:ss a");// a为am/pm的标记
        Date date = new Date();// 获取当前时间
        Connection conn = DailyDeleteToday.getConnection();
        try {
            String sql = "delete from "+ ConfigManager.getProperty("dws.today.table") + " where to_days(now()) - to_days(calculate_time) > 0";
            System.out.println(sql);
            //int r = conn.createStatement().executeUpdate("delete from realtime.dws_tag_today where calculate_time < current_date");
            int r = conn.createStatement().executeUpdate(sql);
            System.out.println(sdf.format(date) + " 删除" + r + "行");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}