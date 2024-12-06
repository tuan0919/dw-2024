package com.nlu.app.exception;

public class ProcessException extends RuntimeException {
    ProcessExceptionEnum enumException;
    public ProcessException(ProcessExceptionEnum enumException) {
        this.enumException = enumException;
    }
}
