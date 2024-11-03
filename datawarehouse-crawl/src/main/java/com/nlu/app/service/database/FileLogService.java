package com.nlu.app.service.database;

import com.nlu.app.constant.LogStatus;
import com.nlu.app.dao.FileLogDAO;
import com.nlu.app.entity.FileLogs;
import org.jdbi.v3.core.Jdbi;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service
public class FileLogService {
    private final FileLogDAO fileLogDAO;

    public FileLogService(@Qualifier("jdbi.db_control") Jdbi jdbi) {
        fileLogDAO = jdbi.onDemand(FileLogDAO.class);
    }

    public long addNewLog(FileLogs log) {
        return fileLogDAO.insert(log);
    }

    public void updateStatus(long log_id, LogStatus status) {
        fileLogDAO.updateStatus(log_id, status);
    }
}
