package com.nlu.app.configuration;

import com.nlu.app.dao.ProcessPropertiesDAO;
import org.jdbi.v3.core.Jdbi;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DAOConfiguration {
    @Bean
    public ProcessPropertiesDAO processPropertiesDAO(@Qualifier("jdbi.db_control") Jdbi jdbi) {
        return jdbi.onDemand(ProcessPropertiesDAO.class);
    }
}
