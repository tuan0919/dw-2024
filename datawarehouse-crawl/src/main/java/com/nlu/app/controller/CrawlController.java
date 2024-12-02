package com.nlu.app.controller;
import com.nlu.app.constant.LogStatus;
import com.nlu.app.entity.FileLogs;
import com.nlu.app.service.database.FileLogService;
import com.nlu.app.service.database.ProcessConfigService;
import com.nlu.app.service.database.ProcessStagingService;
import com.nlu.app.service.process.CellphoneService;
import com.nlu.app.service.process.GearvnService;
import com.nlu.app.service.process.Service;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Controller;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import static com.nlu.app.constant.ProcessConfigConstant.CELLPHONE_CONFIG;
import static com.nlu.app.constant.ProcessConfigConstant.GEARVN_CONFIG;

@Controller
@RequiredArgsConstructor
public class CrawlController {
    private final ProcessConfigService processConfigService;
    private final FileLogService fileLogService;
    private final ProcessStagingService processStagingService;

    public void start_process_crawl(LocalDate date, long config_id) {

    }

    public void start_insert_to_temp_staging(LocalDate date, int config_id) {
        if (date == null) {
            date = LocalDate.now(); // today
        }
        processStagingService.loadCSV_DataToTemp(date, config_id);
    }

    public void start_crawl(long config_id) throws IOException {
        var processConfig = processConfigService.getProcessConfig(config_id);
        Service service = null;
        switch ((int) config_id) {
            case CELLPHONE_CONFIG -> {
                String json = processConfig.getJson_config();
                service = new CellphoneService(json);
            }
            case GEARVN_CONFIG -> {
                String json = processConfig.getJson_config();
                service = new GearvnService(json);
            }
        }
        String today = LocalDate.now()
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
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
