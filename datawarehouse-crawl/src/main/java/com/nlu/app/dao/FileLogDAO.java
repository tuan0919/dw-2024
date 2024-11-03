package com.nlu.app.dao;

import com.nlu.app.constant.LogStatus;
import com.nlu.app.entity.FileLogs;
import com.nlu.app.entity.ProcessConfig;
import org.jdbi.v3.sqlobject.config.RegisterBeanMapper;
import org.jdbi.v3.sqlobject.config.RegisterBeanMappers;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.statement.GetGeneratedKeys;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import java.time.LocalDateTime;

@RegisterBeanMappers({
        @RegisterBeanMapper(LocalDateTime.class),
        @RegisterBeanMapper(FileLogs.class)
})
public interface FileLogDAO {
    @SqlUpdate("""
    INSERT INTO file_logs
    (config_id, time, file_path, count, update_at, status)
    VALUES
    (:log.config_id, :log.time, :log.file_path, :log.count, :log.update_at, :log.status)
    """)
    @GetGeneratedKeys
    Long insert(@BindBean("log") FileLogs fileLogs);

    @SqlUpdate("""
    UPDATE file_logs SET
        status = :status,
        update_at = now()
    WHERE file_log_id = :file_log_id
    """)
    void updateStatus(@Bind("file_log_id") long id, @Bind("status") LogStatus status);
}
