package com.nlu.app.dao.staging;

import com.nlu.app.constant.ProcessConfigConstant;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.sqlobject.SqlObject;

import java.time.LocalDate;

public interface StagingDAO extends SqlObject {
    default void insertToStaging(int config_id) {
        String procedure_name = "";
        switch (config_id) {
            case ProcessConfigConstant.GEARVN_CONFIG -> {
                procedure_name = "transform_and_cleaning_data_gearvn()";
            }
            case ProcessConfigConstant.CELLPHONE_CONFIG -> {
                procedure_name = "transform_and_cleaning_data_cellphone()";
            }
        }
        Handle handle = this.getHandle();
        handle.createCall(String.format("{call dbstaging.%s}", procedure_name))
                .invoke();
    }
}
