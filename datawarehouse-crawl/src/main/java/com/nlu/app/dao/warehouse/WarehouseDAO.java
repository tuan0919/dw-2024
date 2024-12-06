package com.nlu.app.dao.warehouse;

import com.nlu.app.constant.ProcessConfigConstant;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.sqlobject.SqlObject;

public interface WarehouseDAO extends SqlObject {
    default void insertToWarehouse(int config_id) {
        String procedure_name = "";
        switch (config_id) {
            case ProcessConfigConstant.GEARVN_CONFIG -> {
                procedure_name = "load_from_staging_to_dw_gearvn()";
            }
            case ProcessConfigConstant.CELLPHONE_CONFIG -> {
                procedure_name = "load_from_staging_to_dw_cellphone()";
            }
        }
        Handle handle = this.getHandle();
        handle.createCall(String.format("{call datawarehouse.%s}", procedure_name))
                .invoke();
    }
}
