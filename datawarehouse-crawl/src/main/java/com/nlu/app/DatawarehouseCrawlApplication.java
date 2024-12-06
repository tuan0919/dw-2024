package com.nlu.app;
import com.nlu.app.constant.ProcessConfigConstant;
import com.nlu.app.controller.ProcessController;
import com.nlu.app.exception.GlobalHandlerException;
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
    private final ProcessController controller;
    private final ProcessConfigService processConfigService;
    private final FileLogService fileLogService;
    Jdbi jdbi;

    @Autowired
    public void setJdbi(@Qualifier("jdbi.db_staging") Jdbi jdbi) {
        this.jdbi = jdbi;
    }

    public static void main(String[] args) {
        SpringApplication.run(DatawarehouseCrawlApplication.class, args);
    }

    @PostConstruct
    public void test() throws Exception {
//        controller.start_crawl(3);
        try {
//            controller.start_insert_to_temp_staging(LocalDate.of(2024, Month.DECEMBER, 2),
//                    ProcessConfigConstant.GEARVN_CONFIG);
//            controller.start_insert_to_staging(ProcessConfigConstant.GEARVN_CONFIG);
            controller.start_load_to_warehouse(ProcessConfigConstant.GEARVN_CONFIG);
        } catch (Exception e) {
            GlobalHandlerException.handleGlobalException(e);
        }
    }
}
