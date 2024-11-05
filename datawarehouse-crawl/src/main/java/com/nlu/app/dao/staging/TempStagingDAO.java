package com.nlu.app.dao.staging;

import org.jdbi.v3.core.Handle;
import org.jdbi.v3.sqlobject.SqlObject;

public interface TempStagingDAO extends SqlObject {
    default void insertToTempStaging() {
        Handle handle = this.getHandle();
        handle.createCall("{call load_csv_to_temp_staging()}")
                .invoke();
        var procedure = handle.createQuery("SELECT @sql").mapTo(String.class).one();
        handle.execute(procedure);
    }
}
