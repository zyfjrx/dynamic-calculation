package com.byt.tagcalculate.sink;

import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.pool.SessionPool;

import java.util.List;

/**
 * @title:
 * @author: zhangyf
 * @date: 2024/3/06 16:26
 **/
public class IotDBSession {
    public static SessionPool getSession(String host,int port,String user,String pwd,int poolSize) {
        SessionPool session = new SessionPool(host, port, user, pwd, poolSize);
        return session;
    }

    public static Session buildSession(List<String> nodeUrls,String user,String pwd) throws Exception{
        Session session = new Session(nodeUrls, user, pwd);
        session.open();
        return session;
    }
}
