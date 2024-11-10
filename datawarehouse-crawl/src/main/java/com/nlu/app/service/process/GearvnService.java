package com.nlu.app.service.process;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.nlu.app.service.process.crawler.Crawler;
import com.nlu.app.service.process.crawler.Crawler_Cellphones;
import com.nlu.app.service.process.crawler.Crawler_GearVN;
import com.nlu.app.service.process.extractor.Extractor;
import com.nlu.app.service.process.extractor.Extractor_Cellphones;
import com.nlu.app.service.process.extractor.Extractor_Gearvn;

import java.util.List;

public class GearvnService extends Service {
    Crawler crawler;
    Extractor extractor;

    public GearvnService(String propertiesJSON) throws JsonProcessingException {
        super(propertiesJSON);
        this.crawler = new Crawler_GearVN();
        this.extractor = new Extractor_Gearvn();
    }

    @Override
    public String[] crawlProduct(String source) {
        try {
            String html = crawler.prepareHTML(source);
            var fields = crawlProperties.getFields();
            var data = extractor.extract(html, fields);
            super.memory.add(data);
            System.out.println("items in memory: "+super.memory.size());
            return data;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> getSourcesFromPage(String page) {
        return crawler.prepareSources(page);
    }

    public static void main(String[] args) throws JsonProcessingException {
        String html = """
                {
                  "instance": "com.nlu.app.entity.Mouse_GearVN.class",
                  "crawler": "com.nlu.app.service.crawler.Crawler_GearVN.class",
                  "extractor": "com.nlu.app.service.extractor.Extractor_GearVN.class",
                  "fields": [
                    {
                      "field_name": "weight",
                      "number": "single",
                      "attr": "text",
                      "type": "xpath",
                      "selector": "//tr[.//*[contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'), 'trọng lượng')]]/td[2]//span | //ul/li[div[contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'), 'trọng lượng')]]/div[2]"
                    }
                  ]
                }
                """;
        var service = new GearvnService(html);
        var data = service.crawlProduct("https://gearvn.com/products/chuot-dareu-em901x-rgb-superlight-wireless-pink");
        System.out.println(data);
    }
}
