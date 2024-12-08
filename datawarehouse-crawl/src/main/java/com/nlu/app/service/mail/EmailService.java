package com.nlu.app.service.mail;

import com.nlu.app.entity.EmailDetails;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Service;

@Service
public class EmailService {
    private JavaMailSender javaMailSender;
    @Value("${spring.mail.username}") private String sender;

    @Autowired
    private void setJavaMailSender(JavaMailSender javaMailSender) {
        this.javaMailSender = javaMailSender;
    }

    public void sendSimpleMail(String subject, String message) {
        var emailDetails = new EmailDetails();
        emailDetails.setSubject(subject);
        emailDetails.setMsgBody(message);
        emailDetails.setRecipient("nqat0919@gmail.com");
        sendSimpleMail(emailDetails);
    }

    // Method 1
    // To send a simple email
    public String sendSimpleMail(EmailDetails details)
    {

        // Try block to check for exceptions
        try {

            // Creating a simple mail message
            SimpleMailMessage mailMessage
                    = new SimpleMailMessage();

            // Setting up necessary details
            mailMessage.setFrom(sender);
            mailMessage.setTo(details.getRecipient());
            mailMessage.setText(details.getMsgBody());
            mailMessage.setSubject(details.getSubject());

            // Sending the mail
            javaMailSender.send(mailMessage);
            return "Mail Sent Successfully...";
        }

        // Catch block to handle the exceptions
        catch (Exception e) {
            e.printStackTrace();
            return "Error while Sending Mail";
        }
    }
}
