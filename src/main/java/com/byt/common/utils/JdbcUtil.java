package com.byt.common.utils;

import org.apache.commons.dbcp2.BasicDataSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class JdbcUtil {
    private static BasicDataSource dataSource = new BasicDataSource();

    static {
        initSourceConfig();
    }

    private static void initSourceConfig() {
//        dataSource.setDriverClassName("org.postgresql.Driver");
//        dataSource.setUsername(ConfigManager.getProperty("greenplum.opc.user"));
//        dataSource.setPassword(ConfigManager.getProperty("greenplum.opc.pass"));
//        dataSource.setUrl(ConfigManager.getProperty("greenplum.opc.url"));

        // TODO 解决中文名称查询不到结果问题
        String url = "jdbc:mysql://"+ConfigManager.getProperty("mysql.host")+":"+ConfigManager.getProperty("mysql.port")+"/"+ConfigManager.getProperty("mysql.database")+"?autoReconnect=true&useSSL=false&characterEncoding=utf-8";
        System.out.println(url);
        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
        dataSource.setUsername(ConfigManager.getProperty("mysql.username"));
        dataSource.setPassword(ConfigManager.getProperty("mysql.password"));
        dataSource.setUrl(url);

        dataSource.setInitialSize(2);
        dataSource.setMaxTotal(4);
        dataSource.setMaxIdle(2);
        dataSource.setMinIdle(2);
        dataSource.setMaxWaitMillis(6000);
    }

    public static BasicDataSource getDataSource() {
        return dataSource;
    }

    /**
     * 获取连接
     *
     * @return
     */
    public static Connection getConnection() {
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }

    /**
     * 关闭连接串
     *
     * @param conn
     * @param st
     * @param rs
     */
    public static void release(Connection conn, Statement st, ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (st != null) {
            try {
                st.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (conn != null) {
            try {
                //将Connection连接对象还给数据库连接池
                conn.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
