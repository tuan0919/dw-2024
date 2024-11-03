package com.nlu.app.pojo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.List;

@Data @NoArgsConstructor @AllArgsConstructor
public class CrawlProperties {
    String instance;
    String crawler;
    String extractor;
    List<CrawlFields> fields;

    public CrawlProperties(String json) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        var pojo = mapper.readValue(json, CrawlProperties.class);
        this.instance = pojo.instance;
        this.crawler = pojo.crawler;
        this.extractor = pojo.extractor;
        this.fields = pojo.fields;
    }

    public String[] getCSVHeader() {
        int size = fields.size();
        return fields.stream()
                .map(CrawlFields::getField_name)
                .toList().toArray(new String[size]);
    }
}
