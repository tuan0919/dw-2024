package com.nlu.app.dao;

import com.nlu.app.entity.ProcessProperties;
import org.jdbi.v3.sqlobject.config.RegisterBeanMapper;
import org.jdbi.v3.sqlobject.config.RegisterBeanMappers;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.statement.SqlQuery;

import java.time.LocalDateTime;

@RegisterBeanMappers({
        @RegisterBeanMapper(LocalDateTime.class),
        @RegisterBeanMapper(ProcessProperties.class)
})
public interface ProcessPropertiesDAO {
    @SqlQuery("""
    SELECT * FROM process_properties WHERE property_id = :property_id
    """)
    ProcessProperties findOne(@Bind("property_id") long id);
}
