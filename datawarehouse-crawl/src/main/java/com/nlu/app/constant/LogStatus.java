package com.nlu.app.constant;

public enum LogStatus {
    C_E("Crawling"),
    C_SE("Crawl Success"),
    C_FE("Crawl Failure"),
    L_P("Loading"),
    L_SE("Load Success"),
    L_FE("Load Failure"),
    L_CE("Load Completed")
    ;
    final String description;
    LogStatus(String description) {
        this.description = description;
    }
    public String getDescription() {
        return description;
    }
}
