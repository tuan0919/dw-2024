package com.nlu.app.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nlu.app.pojo.CrawlProperties;
import com.nlu.app.service.crawler.Crawler;
import com.nlu.app.service.crawler.Crawler_Cellphones;
import com.nlu.app.service.extractor.Extractor;
import com.nlu.app.service.extractor.Extractor_Cellphones;
import java.util.List;

public class CellphoneService implements Service {
    CrawlProperties crawlProperties;
    Crawler crawler;
    Extractor extractor;

    public CellphoneService(String propertiesJSON) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        this.crawlProperties = objectMapper.readValue(propertiesJSON, CrawlProperties.class);
        this.crawler = new Crawler_Cellphones();
        this.extractor = new Extractor_Cellphones();
    }

    public String[] crawlProduct(String source) {
        try {
            var crawler = new Crawler_Cellphones();
            String html = crawler.prepareHTML(source);
            Extractor_Cellphones extractor = new Extractor_Cellphones();
            var fields = crawlProperties.getFields();
            return extractor.extract(html, fields);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public List<String> getSourcesFromPage(String page) {
        return crawler.prepareSources(page);
    }
}
