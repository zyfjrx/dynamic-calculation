package com.byt.tagcalculate.main;


import com.byt.common.utils.ConfigManager;

import java.sql.Connection;
import java.sql.DriverManager;
import java.text.SimpleDateFormat;
import java.util.Date;


/**
 * @title: 数据清理gp
 * @author: zhangyifan
 * @date: 2022/9/18 16:51
 */

public class DailyDeleteTodayBak {
    private static final String driver = "org.postgresql.Driver";
    private static final String url = ConfigManager.getProperty("greenplum.url");
    private static final String username = ConfigManager.getProperty("greenplum.user");
    private static final String password = ConfigManager.getProperty("greenplum.pass");
    private static Connection conn = null;

    static {
        try {
            Class.forName(driver);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public static Connection getConnection() throws Exception {
        if (conn == null) {
            conn = DriverManager.getConnection(url, username, password); //连接数据库
            return conn;
        }
        return conn;
    }

    public static void main(String[] args) {
        SimpleDateFormat sdf = new SimpleDateFormat();// 格式化时间
        sdf.applyPattern("yyyy-MM-dd HH:mm:ss a");// a为am/pm的标记
        Date date = new Date();// 获取当前时间

        try {
            Connection conn = DailyDeleteTodayBak.getConnection();
            int r = conn.createStatement().executeUpdate("delete from realtime.dws_tag_today where calculate_time < current_date");
            System.out.println(sdf.format(date) + " 删除" + r + "行");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}