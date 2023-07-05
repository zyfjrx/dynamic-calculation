package com.byt.connection;

import java.sql.Connection;

public interface SinkConnection {

    Connection getConnection();

    void close();
}
