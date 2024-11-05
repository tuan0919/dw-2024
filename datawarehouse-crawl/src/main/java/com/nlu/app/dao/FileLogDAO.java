package com.nlu.app.dao;

import com.nlu.app.constant.LogStatus;
import com.nlu.app.entity.FileLogs;
import com.nlu.app.entity.ProcessConfig;
import org.jdbi.v3.core.statement.OutParameters;
import org.jdbi.v3.sqlobject.config.RegisterBeanMapper;
import org.jdbi.v3.sqlobject.config.RegisterBeanMappers;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.customizer.OutParameter;
import org.jdbi.v3.sqlobject.statement.GetGeneratedKeys;
import org.jdbi.v3.sqlobject.statement.SqlCall;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import java.time.LocalDateTime;

@RegisterBeanMappers({
        @RegisterBeanMapper(LocalDateTime.class),
        @RegisterBeanMapper(FileLogs.class)
})
public interface FileLogDAO {
    @SqlCall("""
            call insert_file_logs(
                :log.config_id, :log.file_path,
                :log.count, :log.status,
                :last_id
            )
            """)
    @OutParameter(name = "last_id", sqlType = java.sql.Types.BIGINT)
    OutParameters insert(@BindBean("log") FileLogs fileLogs);

    @SqlQuery("""
            call query_logs_by_id(:file_log_id)
            """)
    FileLogs findOne(@Bind("file_log_id") long id);

    @SqlUpdate("""
        UPDATE file_logs SET
            status = :status,
            update_at = now()
        WHERE file_log_id = :file_log_id
        """)
    void updateStatus(@Bind("file_log_id") long id, @Bind("status") LogStatus status);
}
