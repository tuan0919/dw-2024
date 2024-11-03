package com.nlu.app.service.database;

import com.nlu.app.dao.ProcessPropertiesDAO;
import com.nlu.app.entity.ProcessProperties;
import lombok.RequiredArgsConstructor;
import org.jdbi.v3.core.Jdbi;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service
public class ProcessPropertiesService {
    private final ProcessPropertiesDAO processPropertiesDAO;

    public ProcessPropertiesService(@Qualifier("jdbi.db_control") Jdbi jdbi) {
        this.processPropertiesDAO = jdbi.onDemand(ProcessPropertiesDAO.class);
    }

    public ProcessProperties getProcessProperties(long property_id) {
        return processPropertiesDAO.findOne(property_id);
    }
}
