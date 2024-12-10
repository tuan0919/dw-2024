package com.nlu.app.exception;

import com.nlu.app.entity.MailMessage;

public class ProcessException extends Exception {
    ProcessExceptionEnum enumException;
    MailMessage errorMailMessage;

    public ProcessExceptionEnum getEnumException() {
        return enumException;
    }

    public MailMessage getErrorMailMessage() {
        return errorMailMessage;
    }

    public ProcessException(ProcessExceptionEnum enumException, MailMessage errorMailMessage) {
        this.enumException = enumException;
        this.errorMailMessage = errorMailMessage;
    }

    public ProcessException(ProcessExceptionEnum enumException) {
        this.enumException = enumException;
        this.errorMailMessage = null;
    }
}
