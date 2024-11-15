package com.nlu.app;
import com.nlu.app.constant.LogStatus;
import com.nlu.app.controller.CrawlController;
import com.nlu.app.dao.staging.TempStagingDAO;
import com.nlu.app.entity.FileLogs;
import com.nlu.app.service.database.FileLogService;
import com.nlu.app.service.database.ProcessConfigService;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.jdbi.v3.core.Jdbi;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@RequiredArgsConstructor
public class DatawarehouseCrawlApplication {
    private final CrawlController controller;
    private final ProcessConfigService processConfigService;
    private final FileLogService fileLogService;
    Jdbi jdbi;

    @Autowired
    public void setJdbi(@Qualifier("jdbi.db_staging") Jdbi jdbi) {
        this.jdbi = jdbi;
    }

    public static void main(String[] args) throws Exception {
        SpringApplication.run(DatawarehouseCrawlApplication.class, args);
    }

    @PostConstruct
    public void test() throws Exception {
        controller.start_crawl(1);
    }
}
