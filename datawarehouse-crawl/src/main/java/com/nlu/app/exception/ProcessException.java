package com.nlu.app.exception;

public class ProcessException extends RuntimeException {
    ProcessExceptionEnum enumException;
    String mailMessage;

    public ProcessExceptionEnum getEnumException() {
        return enumException;
    }

    public String getMailMessage() {
        return mailMessage;
    }


    public ProcessException(ProcessExceptionEnum enumException, String mailMessage) {
        this.enumException = enumException;
        this.mailMessage = mailMessage;
    }
}
