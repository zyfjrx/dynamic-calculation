package com.byt.tagcalculate.main;


import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.byt.tagcalculate.connection.impl.DataSourceGetter;
import com.byt.common.utils.ConfigManager;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;


/**
 * @title: 数据清理gp
 * @author: zhangyifan
 * @date: 2022/9/18 16:51
 */

public class DailyDeleteSecond {

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
        Connection conn = DailyDeleteSecond.getConnection();
        try {
            String sql = "truncate table "+ ConfigManager.getProperty("dws.second.table");
            System.out.println(sql);
            int r = conn.createStatement().executeUpdate(sql);
            System.out.println(sdf.format(date) + " 删除" + r + "行");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}