package com.nlu.app.configuration;

import com.mysql.cj.jdbc.MysqlDataSource;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JDBIConfiguration {
    @Value("${config.database.control.CONTROLLER_DB_PORT}")
    private int dbControlPort;
    @Value("${config.database.control.CONTROLLER_DB_NAME}")
    private String dbControlName;
    @Value("${config.database.control.CONTROLLER_DB_PASS}")
    private String dbControlPassword;
    @Value("${config.database.control.CONTROLLER_DB_HOST}")
    private String dbControlHost;

    @Value("${config.database.staging.CONTROLLER_DB_PORT}")
    private int dbStagingPort;
    @Value("${config.database.staging.CONTROLLER_DB_NAME}")
    private String dbStagingName;
    @Value("${config.database.staging.CONTROLLER_DB_PASS}")
    private String dbStagingPassword;
    @Value("${config.database.staging.CONTROLLER_DB_HOST}")
    private String dbStagingHost;

    @Value("${config.database.warehouse.CONTROLLER_DB_PORT}")
    private int dbWarehousePort;
    @Value("${config.database.warehouse.CONTROLLER_DB_NAME}")
    private String dbWarehouseName;
    @Value("${config.database.warehouse.CONTROLLER_DB_PASS}")
    private String dbWarehousePassword;
    @Value("${config.database.warehouse.CONTROLLER_DB_HOST}")
    private String dbWarehouseHost;


    @Bean(value = "jdbi.db_control")
    public Jdbi jdbi_control() {
        var dataSource = new MysqlDataSource();
        dataSource.setURL(String.format(
                "jdbc:mysql://%s:%s/%s",
                dbControlHost, dbControlPort, dbControlName
        ));
        dataSource.setUser("root");
        dataSource.setPassword(dbControlPassword);
        var jdbi = Jdbi.create(dataSource);
        jdbi.installPlugin(new SqlObjectPlugin());
        return jdbi;
    }

    @Bean(value = "jdbi.db_staging")
    public Jdbi jdbi_staging() {
        var dataSource = new MysqlDataSource();
        dataSource.setURL(String.format(
                "jdbc:mysql://%s:%s/%s?allowLoadLocalInfile=true",
                dbStagingHost, dbStagingPort, dbStagingName
        ));
        dataSource.setUser("root");
        dataSource.setPassword(dbStagingPassword);
        var jdbi = Jdbi.create(dataSource);
        jdbi.installPlugin(new SqlObjectPlugin());
        return jdbi;
    }

    @Bean(value = "jdbi.db_warehouse")
    public Jdbi jdbi_warehouse() {
        var dataSource = new MysqlDataSource();
        dataSource.setURL(String.format(
                "jdbc:mysql://%s:%s/%s?allowLoadLocalInfile=true",
                dbWarehouseHost, dbWarehousePort, dbWarehouseName
        ));
        dataSource.setUser("root");
        dataSource.setPassword(dbWarehousePassword);
        var jdbi = Jdbi.create(dataSource);
        jdbi.installPlugin(new SqlObjectPlugin());
        return jdbi;
    }
}
