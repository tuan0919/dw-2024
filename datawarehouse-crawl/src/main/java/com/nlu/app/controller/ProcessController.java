package com.nlu.app.controller;
import com.nlu.app.constant.LogStatus;
import com.nlu.app.entity.FileLogs;
import com.nlu.app.exception.ProcessException;
import com.nlu.app.exception.ProcessExceptionEnum;
import com.nlu.app.service.database.FileLogService;
import com.nlu.app.service.database.ProcessConfigService;
import com.nlu.app.service.database.ProcessStagingService;
import com.nlu.app.service.database.ProcessWarehouseService;
import com.nlu.app.service.mail.EmailService;
import com.nlu.app.service.process.CellphoneService;
import com.nlu.app.service.process.GearvnService;
import com.nlu.app.service.process.Service;
import com.nlu.app.util.MyUtil;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Controller;

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import static com.nlu.app.constant.ProcessConfigConstant.CELLPHONE_CONFIG;
import static com.nlu.app.constant.ProcessConfigConstant.GEARVN_CONFIG;

@Controller
@RequiredArgsConstructor
public class ProcessController {
    private final ProcessConfigService processConfigService;
    private final FileLogService fileLogService;
    private final ProcessStagingService processStagingService;
    private final ProcessWarehouseService processWarehouseService;
    private final EmailService emailService;

    public void start_insert_to_temp(int config_id) {
        start_insert_to_temp(null, config_id);
    }

    public void start_insert_to_temp(LocalDate date, int config_id) {
        if (date == null) {
            date = LocalDate.now(); // today
        }
        processStagingService.loadCSV_DataToTemp(date, config_id);
    }

    public void start_insert_to_staging(int config_id) {
        processStagingService.loadTemp_DataToStaging(config_id);
    }

    public void start_load_to_warehouse(int config_id) {
        processWarehouseService.loadStaging_DataToWarehouse(config_id);
    }

    public void start_crawl(long config_id) throws IOException {
        LocalDateTime startTime = LocalDateTime.now();
        var processConfig = processConfigService.getProcessConfig(config_id);
        String processName = null;
        Service service = null;
        String mailMessage = null;
        switch ((int) config_id) {
            case CELLPHONE_CONFIG -> {
                processName = "Cellphone Crawl";
                String json = processConfig.getJson_config();
                service = new CellphoneService(json);
            }
            case GEARVN_CONFIG -> {
                processName = "GearVN Crawl";
                String json = processConfig.getJson_config();
                service = new GearvnService(json);
            }
        }
        var optional = fileLogService.findOneByDate((int) config_id, LocalDate.now());
        if (optional.isPresent()) {
            var exception = new ProcessException(ProcessExceptionEnum.PROCESS_ALREADY_EXECUTED, mailMessage);
            LocalDateTime endTime = LocalDateTime.now();
            mailMessage = String.format(
                    """
                    Process: %s
                    Tình trạng: thất bại.
                    Thời gian bắt đầu: %s,
                    Thời gian kết thúc: %s,
                    Thông tin thêm:
                        %s
                    Nguyên nhân (nếu có):
                        %s
                    Exception log track trace (nếu có):
                        %s
                    """, processName, MyUtil.formatDateTime(startTime),
                    MyUtil.formatDateTime(endTime), "Không có",
                    "Process đã hoàn thành trước đó",
                    MyUtil.getStackTraceAsString(exception)
            );
            emailService.sendSimpleMail("Crawl Process", mailMessage);
            throw exception;
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
            startTime = LocalDateTime.now();
            var sources = service.getSourcesFromPage(url);
            for (var source : sources) {
                service.crawlProduct(source);
            }
            int count = service.saveMemoryToFile(true, filePath);
            File file = new File(filePath);
            long fileSizeInBytes = file.length();
            int fileSizeInKB = (int) Math.ceil(fileSizeInBytes / 1024.0);
            fileLogService.updateStatus_count_fileSize(log_id, LogStatus.C_SE, count, fileSizeInKB);
        } catch (Exception e) {
            e.printStackTrace();
            fileLogService.updateStatus(log_id, LogStatus.C_FE);
        }
    }
}
