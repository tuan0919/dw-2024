package com.nlu.app.dao.staging;

import com.nlu.app.constant.LogStatus;
import com.nlu.app.constant.ProcessConfigConstant;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.sqlobject.SqlObject;

import java.time.LocalDate;

public interface TempStagingDAO extends SqlObject {
    default void insertToTempStaging(LocalDate date, int config_id) {
        String procedure_name = "";
        switch (config_id) {
            case ProcessConfigConstant.GEARVN_CONFIG -> {
                procedure_name = "load_csv_to_temp_staging_gearvn(:date)";
            }
            case ProcessConfigConstant.CELLPHONE_CONFIG -> {
                procedure_name = "load_csv_to_temp_staging_cellphone(:date)";
            }
        }
        Handle handle = this.getHandle();
        handle.createCall(String.format("{call dbstaging.%s}", procedure_name))
                .bind("date", date)
                .invoke();
        var truncate = handle.createQuery("SELECT @truncate_sql").mapTo(String.class).one();
        var procedure = handle.createQuery("SELECT @load_sql").mapTo(String.class).one();
        var logId = handle.createQuery("SELECT @log_id").mapTo(int.class).one();
        handle.createCall("{call dbcontrol.update_log_status(:log_id, :new_status)}")
                .bind("log_id", logId)
                .bind("new_status", LogStatus.L_P)
                .invoke();
        handle.execute(truncate);
        handle.execute(procedure);
        handle.createCall("{call dbcontrol.update_log_status(:log_id, :new_status)}")
                .bind("log_id", logId)
                .bind("new_status", LogStatus.L_SE)
                .invoke();
    }
}
