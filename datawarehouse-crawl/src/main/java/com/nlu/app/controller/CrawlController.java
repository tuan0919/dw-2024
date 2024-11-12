package com.nlu.app.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.nlu.app.constant.LogStatus;
import com.nlu.app.entity.FileLogs;
import com.nlu.app.entity.ProcessProperties;
import com.nlu.app.service.database.FileLogService;
import com.nlu.app.service.database.ProcessConfigService;
import com.nlu.app.service.database.ProcessPropertiesService;
import com.nlu.app.service.process.CellphoneService;
import com.nlu.app.service.process.FileService;
import com.nlu.app.service.process.GearvnService;
import com.nlu.app.service.process.Service;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.web.format.DateTimeFormatters;
import org.springframework.stereotype.Controller;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

@Controller
@RequiredArgsConstructor
public class CrawlController {
    public static final String CELLPHONE_PROPERTY = "cellphone_properties";
    public static final String GEARVN_PROPERTY = "gearvn_properties";
    private final ProcessConfigService processConfigService;
    private final ProcessPropertiesService processPropertiesService;
    private final FileLogService fileLogService;

    public void start_process_crawl() {

    }

    public void start_crawl(long config_id) throws IOException {
        var processConfig = processConfigService.getProcessConfig(config_id);
        long property_id = processConfig.getProperty_id();
        var properties = processPropertiesService.getProcessProperties(property_id);
        Service service = null;
        switch (properties.getName()) {
            case CELLPHONE_PROPERTY -> {
                String json = properties.getValue();
                service = new CellphoneService(json);
                break;
            }
            case GEARVN_PROPERTY -> {
                String json = properties.getValue();
                service = new GearvnService(json);
                break;
            }
        }
        String today = LocalDate.now()
                .format(DateTimeFormatter.ofPattern("dd-MM-yyyy"));
        String filePath = String.format("%s\\%s_%s.csv",
                processConfig.getSave_location(),
                processConfig.getFile_name(),
                today);
        String logPath = String.format("/allowed_dir/%s_%s.csv",
                processConfig.getFile_name(),
                today);
        assert service != null;
        var log = FileLogs.builder()
                .config_id(config_id)
                .update_at(LocalDateTime.now())
                .count(0)
                .start_time(LocalDateTime.now())
                .file_path(logPath)
                .status(LogStatus.C_E)
                .time(today)
                .build();
        long log_id = fileLogService.addNewLog(log);
        String url = processConfig.getUrl();
        try {
            var sources = service.getSourcesFromPage(url);
            for (var source : sources) {
                service.crawlProduct(source);
            }
            service.saveMemoryToFile(true, filePath);
            fileLogService.updateStatus(log_id, LogStatus.C_SE);
        } catch (Exception e) {
            e.printStackTrace();
            fileLogService.updateStatus(log_id, LogStatus.C_FE);
        }
    }
}
