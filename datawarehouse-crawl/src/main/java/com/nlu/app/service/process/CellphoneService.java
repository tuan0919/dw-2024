package com.nlu.app.service.process;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.nlu.app.service.process.crawler.Crawler;
import com.nlu.app.service.process.crawler.Crawler_Cellphones;
import com.nlu.app.service.process.extractor.Extractor;
import com.nlu.app.service.process.extractor.Extractor_Cellphones;
import java.util.List;

public class CellphoneService extends Service {
    Crawler crawler;
    Extractor extractor;

    public CellphoneService(String propertiesJSON) throws JsonProcessingException {
        super(propertiesJSON);
        this.crawler = new Crawler_Cellphones();
        this.extractor = new Extractor_Cellphones();
    }

    @Override
    public String[] crawlProduct(String source) throws InterruptedException {
        var crawler = new Crawler_Cellphones();
        String html = crawler.prepareHTML(source);
        Extractor_Cellphones extractor = new Extractor_Cellphones();
        var fields = crawlProperties.getFields();
        var data = extractor.extract(html, fields);
        super.memory.add(data);
        System.out.println("items in memory: "+super.memory.size());
        return data;
    }

    @Override
    public List<String> getSourcesFromPage(String page) {
        return crawler.prepareSources(page);
    }

}
