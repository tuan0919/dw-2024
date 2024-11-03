package com.nlu.app.entity;

import lombok.Data;

import java.time.LocalDateTime;

@Data
public class ProcessProperties {
    String property_id;
    String name;
    String header_csv;
    String value;
    LocalDateTime last_modified;
}
