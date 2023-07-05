package com.byt.connection.impl;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.byt.connection.SinkConnection;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.SQLException;

@Slf4j
public class DbConnection implements SinkConnection {
        private final static String SELECT_SQL = "select 1";
        private static DruidPooledConnection pooledConnection;
    private String dbType = "mysql";

    @Override
    public Connection getConnection() {
        if (pooledConnection == null || !check()) {

            DruidDataSource druidDataSource = null;
            if (dbType.equals("mysql")) {
                druidDataSource = DataSourceGetter.getMysqlDataSource();
            } else if (dbType.equals("gp")) {
                druidDataSource = DataSourceGetter.getGpDataSource();
            }
            try {
                assert druidDataSource != null;
                pooledConnection = druidDataSource.getConnection();
            } catch (SQLException e) {
                log.error(dbType + " db connection failed");
                e.printStackTrace();
                throw new RuntimeException(dbType + " db connection failed");
            }
        }
        return pooledConnection.getConnection();
    }

    private boolean check() {
        if (pooledConnection != null) {
            try {
                Connection connection = pooledConnection.getConnection();
                connection.createStatement().execute(SELECT_SQL);
                return true;
            } catch (SQLException e) {
                log.error("check " + dbType + " connection is closed");
            }
        }
        return false;
    }

    @Override
    public void close() {
        if (pooledConnection != null) {
            try {
                pooledConnection.commit();
                pooledConnection.close();
            } catch (SQLException e) {
                log.info(dbType + " connection had closed");
            }
        }
    }
}
