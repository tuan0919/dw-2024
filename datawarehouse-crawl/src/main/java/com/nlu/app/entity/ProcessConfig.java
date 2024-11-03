package com.nlu.app.entity;

import lombok.Data;

@Data
public class ProcessConfig {
    String save_location;
    String tble_warehouse;
    String tble_staging;
    String url;
    String file_name;
    Long schedule;
    String staging_fields;
    String dw_fields;
    String fields_terminated_by;
    String optionally_enclosed_by;
    String lines_terminated_by;
    String ignore_rows;
    String tble_staging_source;
    String property_id;
}
