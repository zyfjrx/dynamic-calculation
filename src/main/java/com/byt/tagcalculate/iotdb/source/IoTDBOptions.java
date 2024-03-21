package com.byt.tagcalculate.iotdb.source;

import java.io.Serializable;

/**
 * @title:
 * @author: zhangyf
 * @date: 2024/3/21 11:10
 **/
public class IoTDBOptions implements Serializable {
    protected String host;
    protected int port;
    protected String user;
    protected String password;

    public IoTDBOptions(String host, int port, String user, String password) {
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
    }

    public IoTDBOptions() {
    }

    public String getHost() {
        return this.host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return this.port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getUser() {
        return this.user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return this.password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
