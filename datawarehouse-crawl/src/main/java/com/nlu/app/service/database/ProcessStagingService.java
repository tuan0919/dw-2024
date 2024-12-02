package com.nlu.app.service.database;

import com.nlu.app.dao.FileLogDAO;
import com.nlu.app.dao.ProcessConfigDAO;
import com.nlu.app.dao.staging.TempStagingDAO;
import com.nlu.app.entity.ProcessConfig;
import org.jdbi.v3.core.Jdbi;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.time.LocalDate;

@Service
public class ProcessStagingService {
    private final TempStagingDAO tempStagingDAO;
    private FileLogDAO fileLogDAO;

    @Autowired
    public void setFileLogDAO(@Qualifier("jdbi.db_control") Jdbi jdbi) {
        this.fileLogDAO = jdbi.onDemand(FileLogDAO.class);
    }

    public ProcessStagingService(@Qualifier("jdbi.db_staging") Jdbi jdbi) {
        this.tempStagingDAO = jdbi.onDemand(TempStagingDAO.class);
    }

    public void loadCSV_DataToTemp(LocalDate date, int config_id) {
        this.tempStagingDAO.insertToTempStaging(date, config_id);
    }
}
