package com.qtech.etl.utils;

import javax.mail.Authenticator;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.List;
import java.util.Properties;

/**
 * author :  gaozhilin
 * email  :  gaoolin@gmail.com
 * date   :  2023/06/16 08:39:43
 * desc   :  邮件发送
 */


public class Email4J {
    public Email4J() {
    }

    public static boolean sendMail(List<String> recipients, String subject, String content) {
        boolean flag = false;

        try {
            final Properties props = System.getProperties();
            System.out.println(props);
            props.put("mail.smtp.auth", "true");
            props.put("mail.smtp.host", "123.58.177.49");
            props.put("mail.smtp.ssl.trust", "123.58.177.49");  // javax.mail.MessagingException: Could not convert socket to TLS;  证书问题
            props.put("mail.user", "bigdata.it@qtechglobal.com");
            props.put("mail.password", "qtech2020");
            props.put("mail.smtp.port", "25");
            /*props.put("mail.smtp.starttls.enable", "true");*/
            Authenticator authenticator = new Authenticator() {
                protected PasswordAuthentication getPasswordAuthentication() {
                    String userName = props.getProperty("mail.user");
                    String password = props.getProperty("mail.password");
                    return new PasswordAuthentication(userName, password);
                }
            };
            Session session = Session.getInstance(props, authenticator);
            MimeMessage message = new MimeMessage(session);
            InternetAddress fromAddress = new InternetAddress(props.getProperty("mail.user"));
            message.setFrom(fromAddress);
            int num = recipients.size();
            InternetAddress[] addresses = new InternetAddress[num];

            for (int i = 0; i < num; ++i) {
                addresses[i] = new InternetAddress((String) recipients.get(i));
            }

            message.setRecipients(MimeMessage.RecipientType.TO, addresses);
            message.setSubject(subject);
            message.setContent(content, "text/html;charset=UTF-8");
            Transport.send(message);
            flag = true;
        } catch (Exception var12) {
            var12.printStackTrace();
        }

        return flag;
    }
}
