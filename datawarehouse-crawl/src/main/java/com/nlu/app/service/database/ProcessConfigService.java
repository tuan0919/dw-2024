package com.nlu.app.service.database;

import com.nlu.app.dao.ProcessConfigDAO;
import com.nlu.app.entity.ProcessConfig;
import lombok.RequiredArgsConstructor;
import org.jdbi.v3.core.Jdbi;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class ProcessConfigService {
    private final ProcessConfigDAO processConfigDAO;

    public ProcessConfigService(@Qualifier("jdbi.db_control") Jdbi jdbi) {
        this.processConfigDAO = jdbi.onDemand(ProcessConfigDAO.class);
    }

    public ProcessConfig getProcessConfig(long id) {
        return processConfigDAO.findOne(id);
    }
}
