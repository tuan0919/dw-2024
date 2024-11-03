package com.nlu.app.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nlu.app.pojo.CrawlProperties;
import com.nlu.app.service.crawler.Crawler;
import com.nlu.app.service.crawler.Crawler_Cellphones;
import com.nlu.app.service.extractor.Extractor;
import com.nlu.app.service.extractor.Extractor_Cellphones;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
public class CellphoneService {
    CrawlProperties properties;
    Crawler crawler;
    Extractor extractor;
    final FileService csvWriter;

    @Autowired
    public void setCrawler(@Qualifier("cellphones-crawler") Crawler crawler) {
        this.crawler = crawler;
    }

    @Autowired
    public void setExtractor(@Qualifier("cellphones-extractor") Extractor extractor) {
        this.extractor = extractor;
    }

    @Autowired
    public void setProperties(@Value("classpath:cellphones.json") Resource resource) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        this.properties = objectMapper.readValue(resource.getInputStream(), CrawlProperties.class);
    }

    public void crawlProduct(String source) {
        try {
            var crawler = new Crawler_Cellphones();
            String html = crawler.prepareHTML(source);
            Extractor_Cellphones extractor = new Extractor_Cellphones();
            var fields = properties.getFields();
            String[] header = properties.getCSVHeader();
            List<String[]> rows = new ArrayList<>();
            rows.add(extractor.extract(html, fields));
            csvWriter.writeCSV(header, rows, "E:\\Temp\\crawl.csv");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public List<String> getSourcesFromPage(String page) {
        return crawler.prepareSources(page);
    }
}
