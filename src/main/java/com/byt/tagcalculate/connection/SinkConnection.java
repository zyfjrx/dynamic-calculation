package com.byt.tagcalculate.connection;

import java.sql.Connection;

public interface SinkConnection {

    Connection getConnection();

    void close();
}
