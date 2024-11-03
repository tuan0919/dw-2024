package com.nlu.app.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ProcessProperties {
    String property_id;
    String name;
    String header_csv;
    String value;
    LocalDateTime last_modified;
}
