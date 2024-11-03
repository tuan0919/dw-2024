package com.nlu.app.constant;

public enum LogStatus {
    C_RE("Crawl Ready"),
    C_E("Crawling"),
    C_SE("Crawl Success"),
    C_FE("Crawl Failure"),
    ;
    final String description;
    LogStatus(String description) {
        this.description = description;
    }
    public String getDescription() {
        return description;
    }
}
