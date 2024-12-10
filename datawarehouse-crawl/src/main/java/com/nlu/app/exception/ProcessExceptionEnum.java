package com.nlu.app.exception;

public enum ProcessExceptionEnum {
    FILE_NOT_EXISTS_OR_NOT_READY(45000, "File is not existed or this process is already executed."),
    PROCESS_ALREADY_EXECUTED(46000, "Process is already executed."),
    UNKNOWN_EXCEPTION_HAPPEN(99999, "Something went wrong when execute this process."),
    JSON_PROCESSING_EXCEPTION(47000, "Json format for properties is not valid.")
    ;
    private int code;
    private String message;

    public String getMessage() {
        return message;
    }

    ProcessExceptionEnum(int code, String message) {
        this.code = code;
        this.message = message;
    }

    public static ProcessExceptionEnum getBasedOnCode(int code) {
        switch (code) {
            case 45000 -> {
                return FILE_NOT_EXISTS_OR_NOT_READY;
            }
        }
        return null;
    }
}
