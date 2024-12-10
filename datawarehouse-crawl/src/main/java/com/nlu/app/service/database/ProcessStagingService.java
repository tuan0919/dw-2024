package com.nlu.app.service.database;

import com.nlu.app.dao.FileLogDAO;
import com.nlu.app.dao.ProcessConfigDAO;
import com.nlu.app.dao.staging.StagingDAO;
import com.nlu.app.dao.staging.TempStagingDAO;
import com.nlu.app.entity.ProcessConfig;
import com.nlu.app.exception.ProcessExceptionEnum;
import org.apache.commons.lang3.exception.UncheckedException;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.statement.UnableToExecuteStatementException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.time.LocalDate;

@Service
public class ProcessStagingService {
    private final TempStagingDAO tempStagingDAO;
    private StagingDAO stagingDAO;
    private FileLogDAO fileLogDAO;

    @Autowired
    public void setFileLogDAO(@Qualifier("jdbi.db_control") Jdbi jdbi) {
        this.fileLogDAO = jdbi.onDemand(FileLogDAO.class);
    }



    public ProcessStagingService(@Qualifier("jdbi.db_staging") Jdbi jdbi) {
        this.tempStagingDAO = jdbi.onDemand(TempStagingDAO.class);
        this.stagingDAO = jdbi.onDemand(StagingDAO.class);
    }

    public void loadCSV_DataToTemp(LocalDate date, int config_id) throws SQLException {
        try {
            this.tempStagingDAO.insertToTempStaging(date, config_id);
        } catch (UnableToExecuteStatementException exception) {
            if (exception.getCause() instanceof SQLException) {
                var e = (SQLException) exception.getCause();
                var errorCode = Integer.parseInt(e.getSQLState());
                var processException = ProcessExceptionEnum.getBasedOnCode(errorCode);
                if (processException != null) {
                    throw e;
                }
                else {
                    throw new UncheckedException(exception);
                }
            }
        }
    }

    public void loadTemp_DataToStaging(int config_id) {
        this.stagingDAO.insertToStaging(config_id);
    }
}
