package com.nlu.app.dao.staging;

import org.jdbi.v3.core.Handle;
import org.jdbi.v3.sqlobject.SqlObject;

import java.time.LocalDate;

public interface TempStagingDAO extends SqlObject {
    default void insertToTempStaging(LocalDate date) {
        Handle handle = this.getHandle();
        handle.createCall("{call load_csv_to_temp_staging(:date)}")
                .bind("date", date)
                .invoke();
        var procedure = handle.createQuery("SELECT @sql").mapTo(String.class).one();
        var logId = handle.createQuery("SELECT @log_id").mapTo(int.class).one();
        handle.execute(procedure);
        handle.createCall("{call update_log_status(:log_id, :new_status)}")
                .bind("log_id", logId)
                .bind("new_status", "L_SE")
                .invoke();
    }
}
