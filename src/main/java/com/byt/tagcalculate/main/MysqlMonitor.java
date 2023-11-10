package com.byt.tagcalculate.main;


import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.byt.common.utils.ConfigManager;
import com.byt.common.utils.LinuxUtil;
import com.byt.tagcalculate.connection.impl.DataSourceGetter;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;


/**
 * @title: mysql数据监控
 * @author: zhangyifan
 * @date: 2022/9/18 16:51
 */

public class MysqlMonitor {

    private final static String SELECT_SQL = "select 1";
    private static DruidPooledConnection pooledConnection;

    public static Connection getConnection() throws Exception {
        if (pooledConnection == null) {
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
        Connection conn = MysqlMonitor.getConnection();
        try {
            String sql = "select * from " + ConfigManager.getProperty("dws.today.table") + " " +
                    " WHERE calculate_time >= DATE_SUB(NOW(), INTERVAL 2 HOUR) ";
            System.out.println(sql);
            ResultSet resultSet = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE).executeQuery(sql);
            resultSet.last();
            int row = resultSet.getRow();
            System.out.println(row);
            if (row <= 0) {
                String str = LinuxUtil.execCommand(
                        ConfigManager.getProperty("flink.host"),
                        ConfigManager.getProperty("flink.user"),
                        ConfigManager.getProperty("flink.pwd"),
                        " sh /opt/flink-project/monitorjob.sh");
                System.out.println(str);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}