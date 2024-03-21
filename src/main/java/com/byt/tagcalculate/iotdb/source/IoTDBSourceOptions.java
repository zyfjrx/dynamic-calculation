package com.byt.tagcalculate.iotdb.source;



/**
 * @title:
 * @author: zhangyf
 * @date: 2024/3/21 11:09
 **/
public class IoTDBSourceOptions extends IoTDBOptions {
    private String sql;
    private int fetchSize;

    public IoTDBSourceOptions(String host, int port, String user, String password, String sql) {
        this(host, port, user, password, sql, 1024);
    }

    public IoTDBSourceOptions(String host, int port, String user, String password, String sql, int fetchSIze) {
        super(host, port, user, password);
        this.sql = sql;
        this.fetchSize = fetchSIze;
    }

    public String getSql() {
        return this.sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public int getFetchSize() {
        return this.fetchSize;
    }

    public void setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }
}

