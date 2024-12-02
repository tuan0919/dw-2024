package com.nlu.app.dao;

import com.nlu.app.entity.ProcessConfig;
import org.jdbi.v3.sqlobject.config.RegisterBeanMapper;
import org.jdbi.v3.sqlobject.config.RegisterBeanMappers;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.statement.SqlQuery;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@RegisterBeanMappers({
        @RegisterBeanMapper(LocalDateTime.class),
        @RegisterBeanMapper(ProcessConfig.class)
})
public interface ProcessConfigDAO {
    @SqlQuery("""
    SELECT * FROM configs WHERE config_id = :config_id
    """)
    ProcessConfig findOne(@Bind("config_id") long id);

    @SqlQuery("""
    SELECT * FROM configs
    """)
    List<ProcessConfig> findAll();
}
