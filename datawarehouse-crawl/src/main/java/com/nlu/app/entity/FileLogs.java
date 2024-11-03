package com.nlu.app.entity;
import com.nlu.app.constant.LogStatus;
import lombok.Data;
import java.time.LocalDateTime;

@Data
public class FileLogs {
    Long file_log_id;
    Long config_id;
    String time;
    String file_path;
    Integer count;
    LocalDateTime start_time;
    LocalDateTime end_time;
    Integer file_size;
    LocalDateTime update_at;
    LogStatus status;
}
