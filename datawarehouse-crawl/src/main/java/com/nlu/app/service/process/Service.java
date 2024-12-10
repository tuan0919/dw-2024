package com.nlu.app.service.process;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nlu.app.pojo.CrawlProperties;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public abstract class Service {
    final CrawlProperties crawlProperties;
    final FileService fileService;
    List<String[]> memory;

    public Service(String propertiesJSON) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        this.crawlProperties = objectMapper.readValue(propertiesJSON, CrawlProperties.class);
        this.fileService = new FileService();
        this.memory = new ArrayList<>();
    }

    public abstract String[] crawlProduct(String source) throws InterruptedException;
    public abstract List<String> getSourcesFromPage(String page);
    public int saveMemoryToFile(boolean resetMemory, String filePath) throws IOException {
        String[] header = crawlProperties.getCSVHeader();
        var result = fileService.writeCSV(header, this.memory, filePath);
        if (resetMemory) {
            this.memory.clear();
        }
        return result;
    }
}
