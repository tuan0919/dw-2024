package com.nlu.app.entity;

import lombok.Builder;
import lombok.Data;

import java.time.LocalDateTime;

@Data @Builder
public class MailMessage {
    String subject;
    String processName;
    String status;
    LocalDateTime startTime;
    LocalDateTime endTime;
    String note;
    String reason;
    String exceptionTrace;
}
