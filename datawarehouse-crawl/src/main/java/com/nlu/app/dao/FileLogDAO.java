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

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Optional;

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

    @SqlUpdate("""
        call update_log_status_count_fileSize(
            :file_log_id, :new_status,
            :new_count, :new_file_size
        )
        """)
    void updateStatus_count_fileSize(@Bind("file_log_id") long id,
                                     @Bind("new_status") LogStatus status,
                                     @Bind("new_count") int count,
                                     @Bind("new_file_size") int fileSize);

    @SqlQuery("""
            call query_logs_to_check_crawl_execution(:config_id, :create_date)
            """)
    Optional<FileLogs> findOneByDate(int config_id, LocalDate create_date);
}
