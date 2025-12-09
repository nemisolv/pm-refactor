package com.viettel.schedule;

import jakarta.activation.DataHandler;
import jakarta.activation.FileDataSource;
import jakarta.mail.Message;
import jakarta.mail.Multipart;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeBodyPart;
import jakarta.mail.internet.MimeMessage;
import jakarta.mail.internet.MimeMultipart;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.MailException;
import org.springframework.mail.javamail.JavaMailSenderImpl;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.Objects;


@Component
@Slf4j
public class EmailSender {

    @Value("${spring.mail.properties.mail.retry:0}")
    private int retry;

    private final JavaMailSenderImpl mailSender;

    @Autowired
    public EmailSender(JavaMailSenderImpl mailSender) {
        this.mailSender = mailSender;
    }


    public void sendManyPeopleAttachment(
        String[] to,
        String subject,
        String body,
        String... filePath
    ) throws Exception {
        MimeMessage msg = mailSender.createMimeMessage();

        msg.setFrom(Objects.requireNonNull(mailSender.getUsername()));
        InternetAddress[] addressTo = new InternetAddress[to.length];
        for(int i =0;i < to.length;i++) {
            addressTo[i] = new InternetAddress(to[i]);
        }
        msg.setRecipients(Message.RecipientType.TO, addressTo);


        msg.setSubject(subject);
        MimeBodyPart mbp1 = new MimeBodyPart();
        mbp1.setText(body, "utf-8", "html");

        Multipart mp = new MimeMultipart();
        mp.addBodyPart(mbp1);
        for(int i = 0;i < filePath.length;i++) {
            MimeBodyPart mbp2 = new MimeBodyPart();
            FileDataSource fds = new FileDataSource(filePath[i]);
            mbp2.setDataHandler(new DataHandler(fds));
            mbp2.setFileName(fds.getName());

            mp.addBodyPart(mbp2);
        }

        msg.setContent(mp);

        msg.setHeader("X-Mailer","LOTONtechEmail");
        msg.setSentDate(new Date());

        msg.saveChanges();

        for(int i = 0;i <= retry;i++) {
            try {
                mailSender.send(msg);
                break;
            }catch(MailException ex) {
                log.error("Sent mail fialed, retry time: {}", i);
            }
        }



    }



}
