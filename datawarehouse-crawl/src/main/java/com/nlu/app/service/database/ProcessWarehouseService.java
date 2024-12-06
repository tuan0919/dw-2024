package com.nlu.app.service.database;

import com.nlu.app.dao.FileLogDAO;
import com.nlu.app.dao.staging.StagingDAO;
import com.nlu.app.dao.staging.TempStagingDAO;
import com.nlu.app.dao.warehouse.WarehouseDAO;
import org.jdbi.v3.core.Jdbi;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.time.LocalDate;

@Service
public class ProcessWarehouseService {
    private final WarehouseDAO warehouseDAO;

    public ProcessWarehouseService(@Qualifier("jdbi.db_warehouse") Jdbi jdbi) {
        this.warehouseDAO = jdbi.onDemand(WarehouseDAO.class);
    }

    public void loadStaging_DataToWarehouse(int config_id) {
        warehouseDAO.insertToWarehouse(config_id);
    }
}
