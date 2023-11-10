package com.byt.common.utils;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.Session;
import ch.ethz.ssh2.StreamGobbler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

/**
 * @title:
 * @author: zhangyf
 * @date: 2023/11/10 8:44
 **/
public class LinuxUtil {

    private static Connection login(String hostname, int port , String username, String password){
        try {
            Connection conn = new Connection(hostname,port);
            conn.connect();
            boolean authenticate = conn.authenticateWithPassword(username, password);
            if (authenticate == false){
                throw new IOException("登录服务器失败");
            }
            return conn;
        }catch (IOException e){
            e.printStackTrace();
        }
        return null;

    }

    private static Session getSession(Connection conn){
        Session sess = null;
        try {
            sess = conn.openSession();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return sess;

    }

    private static String exec(String hostname,String username,String password,String cmd,boolean result){
        Connection login = null;
        Session session = null;
        try {
            login = login(hostname, 22, username, password);
            if (login != null){
                session = getSession(login);
                session.execCommand(cmd);
                if (result){
                    return processStdout(session.getStdout());
                }
            }
        }catch (IOException e){
            e.printStackTrace();
        }
        return null;

    }


    public static String execCommand(String hostname,String username,String password,String cmd){
        return exec(hostname,username,password,cmd,true);
    }


    private static String processStdout(InputStream in){
        InputStream stdout = new StreamGobbler(in);
        StringBuffer buffer = new StringBuffer();
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(stdout, StandardCharsets.UTF_8));
            String line;
            while ((line = br.readLine())!= null){
                buffer.append(line+"\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return buffer.toString();
    }

    public static void main(String[] args) {
        String command = execCommand("hadoop202", "root", "123456",
                " sh /opt/module/project/test.sh");
        System.out.println(command);
    }

}
