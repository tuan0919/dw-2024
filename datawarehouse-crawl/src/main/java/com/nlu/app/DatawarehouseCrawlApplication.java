package com.nlu.app;
import com.nlu.app.constant.ProcessConfigConstant;
import com.nlu.app.controller.ProcessController;
import com.nlu.app.entity.EmailDetails;
import com.nlu.app.exception.GlobalHandlerException;
import com.nlu.app.service.mail.EmailService;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@SpringBootApplication
@RequiredArgsConstructor
public class DatawarehouseCrawlApplication {
    private final ProcessController controller;
    private final EmailService emailService;

    public static void main(String[] args) {
        SpringApplication.run(DatawarehouseCrawlApplication.class, args);
    }

    @PostConstruct
    public void test() {
        try {
            controller.start_crawl(ProcessConfigConstant.GEARVN_CONFIG);
//            controller.start_insert_to_temp(ProcessConfigConstant.CELLPHONE_CONFIG);
////
//            controller.start_insert_to_staging(ProcessConfigConstant.CELLPHONE_CONFIG);
//            controller.start_load_to_warehouse(ProcessConfigConstant.CELLPHONE_CONFIG);
        } catch (Exception e) {
            GlobalHandlerException.handleGlobalException(e);
        }
    }
}
