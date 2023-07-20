package com.byt.common.utils;

import javax.mail.*;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Properties;

/**
 * @title:
 * @author: zhangyf
 * @date: 2023/6/9 13:59
 **/
public class MailUtil {
    public static String smtpServer = "smtp.126.com";
    public static String fromEmail = "zhangyf_coder@126.com";
    public static String password = "NQUXXMWQNSRKPRZK";
    public static String toEmail = "zhangyfcoder@163.com";
    public static String subject = "flink预警";

    public static void sendEmail(String message) throws AddressException,
            MessagingException {

        // SMTP服务器信息
        Properties props = new Properties();
        props.put("mail.transport.protocol", "smtp");
        props.put("mail.smtp.host", smtpServer);
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.port", "465");
        props.put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
        props.put("mail.smtp.socketFactory.fallback", false);
        props.put("mail.smtp.socketFactory.port", "465");
        props.put("mail.smtp.starttls.enable", "true");

        // 创建新的Session对象
        Session session = Session.getInstance(props, new Authenticator() {
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(fromEmail, password);
            }
        });

        // 创建新的MimeMessage对象
        Message msg = new MimeMessage(session);

        // 设置发件人和收件人
        msg.setFrom(new InternetAddress(fromEmail));
        msg.addRecipient(Message.RecipientType.TO, new InternetAddress(toEmail));

        // 设置邮件主题和正文
        msg.setSubject(subject);
        msg.setText(message);

        // 发送邮件
        Transport.send(msg);
        System.out.println("发送成功");
    }

    public static void main(String[] args) throws MessagingException {
        MailUtil.sendEmail("测试");
    }

}
