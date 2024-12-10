package com.nlu.app;
import com.nlu.app.controller.ProcessController;
import com.nlu.app.exception.ProcessException;
import com.nlu.app.pojo.ApplicationProcessConfig;
import com.nlu.app.service.mail.EmailService;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableAsync
public class DatawarehouseCrawlApplication {
    private ProcessController controller;
    private ApplicationProcessConfig processConfig;
    private EmailService emailService;

    @PostConstruct
    public void start() {
        for (var cf : processConfig.getProcess()) {
            start_crawl_data(cf.getConfigId());
        }
    }

    public static void main(String[] args) {
        SpringApplication.run(DatawarehouseCrawlApplication.class, args);
    }

    @Autowired
    public void setProcessConfig(ApplicationProcessConfig processConfig) {
        this.processConfig = processConfig;
    }

    @Autowired
    public void setController(ProcessController controller) {
        this.controller = controller;
    }

    @Autowired
    public void setEmailService(EmailService emailService) {
        this.emailService = emailService;
    }

    @Async
    public void start_load_to_staging(int config_id) {
        try {
            controller.start_insert_to_staging(config_id);
        } catch (ProcessException exception) {
            emailService.sendSimpleMail(exception.getErrorMailMessage());
        }
    }

    @Async
    public void start_crawl_data(int config_id) {
        try {
            controller.start_crawl(config_id);
        } catch (ProcessException exception) {
            emailService.sendSimpleMail(exception.getErrorMailMessage());
        }
    }

    @Async
    public void start_load_data_to_temp(int config_id) {
        try {
            controller.start_insert_to_temp(config_id);
        } catch (ProcessException exception) {
            emailService.sendSimpleMail(exception.getErrorMailMessage());
        }
    }

    @Async
    public void start_load_data_to_warehouse(int config_id) {
        try {
            controller.start_load_to_warehouse(config_id);
        } catch (ProcessException exception) {
            emailService.sendSimpleMail(exception.getErrorMailMessage());
        }
    }
}
