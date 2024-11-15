package com.nlu.app.configuration;

import com.mysql.cj.jdbc.MysqlDataSource;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JDBIConfiguration {
    @Bean(value = "jdbi.db_control")
    public Jdbi jdbi_control() {
        var dataSource = new MysqlDataSource();
        dataSource.setURL("jdbc:mysql://localhost:3307/dbcontrol");
        dataSource.setUser("root");
        dataSource.setPassword("123");
        var jdbi = Jdbi.create(dataSource);
        jdbi.installPlugin(new SqlObjectPlugin());
        return jdbi;
    }

    @Bean(value = "jdbi.db_staging")
    public Jdbi jdbi_staging() {
        var dataSource = new MysqlDataSource();
        dataSource.setURL("jdbc:mysql://localhost:3307/dbstaging?allowLoadLocalInfile=true");
        dataSource.setUser("root");
        dataSource.setPassword("123");
        var jdbi = Jdbi.create(dataSource);
        jdbi.installPlugin(new SqlObjectPlugin());
        return jdbi;
    }
}
