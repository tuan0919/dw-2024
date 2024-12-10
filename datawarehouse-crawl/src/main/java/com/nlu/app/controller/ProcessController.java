package com.nlu.app.controller;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.nlu.app.constant.LogStatus;
import com.nlu.app.entity.FileLogs;
import com.nlu.app.entity.MailMessage;
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
import java.sql.SQLException;
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

    public void start_insert_to_temp(int config_id) throws ProcessException {
        start_insert_to_temp(null, config_id);
    }

    public void start_insert_to_temp(LocalDate date, int config_id) throws ProcessException {
        LocalDateTime startTime = LocalDateTime.now();
        LocalDateTime endTime;
        MailMessage mailMessage;
        String processName = null ;
        String subject = "Insert To Temp Process Report";
        switch (config_id) {
            case CELLPHONE_CONFIG -> {
                processName = "Cellphone Insert To Temp";
            }
            case GEARVN_CONFIG -> {
                processName = "GearVN Insert To Temp";
            }
        }

        if (date == null) {
            date = LocalDate.now(); // today
        }
        try {
            processStagingService.loadCSV_DataToTemp(date, config_id);
        } catch ( SQLException e) {
            var errorCode = Integer.parseInt(e.getSQLState());
            var processException = ProcessExceptionEnum.getBasedOnCode(errorCode);
            String reason = processException.getMessage();
            endTime = LocalDateTime.now();
            mailMessage = MailMessage.builder()
                    .processName(processName)
                    .status("Failed")
                    .subject(subject)
                    .startTime(startTime)
                    .endTime(endTime)
                    .note("None")
                    .reason(reason)
                    .exceptionTrace(MyUtil.getStackTraceAsString(e))
                    .build();
            throw new ProcessException(processException, mailMessage);
        } catch ( Exception e) {
            endTime = LocalDateTime.now();
            mailMessage = MailMessage.builder()
                    .processName(processName)
                    .status("Failed")
                    .subject(subject)
                    .startTime(startTime)
                    .endTime(endTime)
                    .note("None")
                    .reason(ProcessExceptionEnum.UNKNOWN_EXCEPTION_HAPPEN.getMessage())
                    .exceptionTrace(MyUtil.getStackTraceAsString(e))
                    .build();
            throw new ProcessException(ProcessExceptionEnum.UNKNOWN_EXCEPTION_HAPPEN, mailMessage);
        }
        endTime = LocalDateTime.now();
        mailMessage = MailMessage.builder()
                .processName(processName)
                .status("Success")
                .subject(subject)
                .startTime(startTime)
                .endTime(endTime)
                .note("None")
                .reason("None")
                .exceptionTrace("None")
                .build();
        emailService.sendSimpleMail(mailMessage);
    }

    public void start_insert_to_staging(int config_id) throws ProcessException {
        LocalDateTime startTime = LocalDateTime.now();
        LocalDateTime endTime;
        MailMessage mailMessage = null;
        String processName = null;
        String subject = "Insert to Staging Process Report";
        switch (config_id) {
            case CELLPHONE_CONFIG -> {
                processName = "Cellphone Insert To Staging";
            }
            case GEARVN_CONFIG -> {
                processName = "GearVN Insert To Staging";
            }
        }
        try {
            processStagingService.loadTemp_DataToStaging(config_id);
            endTime = LocalDateTime.now();
            mailMessage = MailMessage.builder()
                    .processName(processName)
                    .status("Success")
                    .subject(subject)
                    .startTime(startTime)
                    .endTime(endTime)
                    .note("None")
                    .reason("None")
                    .exceptionTrace("None")
                    .build();
        } catch (Exception e) {
            endTime = LocalDateTime.now();
            mailMessage = MailMessage.builder()
                    .processName(processName)
                    .status("Failed")
                    .subject(subject)
                    .startTime(startTime)
                    .endTime(endTime)
                    .note("None")
                    .reason(ProcessExceptionEnum.UNKNOWN_EXCEPTION_HAPPEN.getMessage())
                    .exceptionTrace(MyUtil.getStackTraceAsString(e))
                    .build();
            throw new ProcessException(ProcessExceptionEnum.UNKNOWN_EXCEPTION_HAPPEN, mailMessage);
        }
        emailService.sendSimpleMail(mailMessage);
    }

    public void start_load_to_warehouse(int config_id) throws ProcessException {
        LocalDateTime startTime = LocalDateTime.now();
        LocalDateTime endTime;
        MailMessage mailMessage = null;
        String processName = null;
        String subject = "Insert To Warehouse Process Report";
        switch (config_id) {
            case CELLPHONE_CONFIG -> {
                processName = "Cellphone Insert To Warehouse";
            }
            case GEARVN_CONFIG -> {
                processName = "GearVN Insert To Warehouse";
            }
        }
        try {
            processWarehouseService.loadStaging_DataToWarehouse(config_id);
            endTime = LocalDateTime.now();
            mailMessage = MailMessage.builder()
                    .processName(processName)
                    .status("Success")
                    .subject(subject)
                    .startTime(startTime)
                    .endTime(endTime)
                    .note("None")
                    .reason("None")
                    .exceptionTrace("None")
                    .build();
        } catch (Exception e) {
            endTime = LocalDateTime.now();
            mailMessage = MailMessage.builder()
                    .processName(processName)
                    .status("Failed")
                    .subject(subject)
                    .startTime(startTime)
                    .endTime(endTime)
                    .note("None")
                    .reason(ProcessExceptionEnum.UNKNOWN_EXCEPTION_HAPPEN.getMessage())
                    .exceptionTrace(MyUtil.getStackTraceAsString(e))
                    .build();
            throw new ProcessException(ProcessExceptionEnum.UNKNOWN_EXCEPTION_HAPPEN, mailMessage);
        }
        emailService.sendSimpleMail(mailMessage);
    }

    public void start_crawl(long config_id) throws ProcessException {
        LocalDateTime startTime = LocalDateTime.now();
        LocalDateTime endTime;
        String processName = null;
        Service service = null;
        MailMessage mailMessage = null;
        String subject = "Crawl Process Report";
        try {
            var processConfig = processConfigService.getProcessConfig(config_id);
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
                endTime = LocalDateTime.now();
                mailMessage = MailMessage.builder()
                        .processName(processName)
                        .status("Failed")
                        .subject(subject)
                        .startTime(startTime)
                        .endTime(endTime)
                        .note("None")
                        .reason(ProcessExceptionEnum.PROCESS_ALREADY_EXECUTED.getMessage())
                        .exceptionTrace("None")
                        .build();
                throw new ProcessException(ProcessExceptionEnum.PROCESS_ALREADY_EXECUTED, mailMessage);
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
                endTime = LocalDateTime.now();
                mailMessage = MailMessage.builder()
                        .processName(processName)
                        .status("Success")
                        .subject(subject)
                        .startTime(startTime)
                        .endTime(endTime)
                        .note(String.format("count: %s, file size: %s kb", count, fileSizeInKB))
                        .reason("None")
                        .exceptionTrace("None")
                        .build();
                emailService.sendSimpleMail(mailMessage);
            } catch (Exception e) {
                e.printStackTrace();
                fileLogService.updateStatus(log_id, LogStatus.C_FE);
                throw e;
            }
        } catch (JsonProcessingException e) {
            endTime = LocalDateTime.now();
            mailMessage = MailMessage.builder()
                    .processName(processName)
                    .status("Failed")
                    .subject(subject)
                    .startTime(startTime)
                    .endTime(endTime)
                    .note("None")
                    .reason(ProcessExceptionEnum.JSON_PROCESSING_EXCEPTION.getMessage())
                    .exceptionTrace(MyUtil.getStackTraceAsString(e))
                    .build();
            throw new ProcessException(ProcessExceptionEnum.JSON_PROCESSING_EXCEPTION, mailMessage);
        } catch (ProcessException e) {
            throw e;
        } catch (Exception e) {
            endTime = LocalDateTime.now();
            mailMessage = MailMessage.builder()
                    .processName(processName)
                    .status("Failed")
                    .subject(subject)
                    .startTime(startTime)
                    .endTime(endTime)
                    .note("None")
                    .reason(ProcessExceptionEnum.UNKNOWN_EXCEPTION_HAPPEN.getMessage())
                    .exceptionTrace(MyUtil.getStackTraceAsString(e))
                    .build();
            throw new ProcessException(ProcessExceptionEnum.UNKNOWN_EXCEPTION_HAPPEN, mailMessage);
        }
    }
}
