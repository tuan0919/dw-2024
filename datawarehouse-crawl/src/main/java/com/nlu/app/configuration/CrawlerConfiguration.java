package com.nlu.app.configuration;

import com.nlu.app.service.process.crawler.Crawler;
import com.nlu.app.service.process.crawler.Crawler_Cellphones;
import com.nlu.app.service.process.extractor.Extractor;
import com.nlu.app.service.process.extractor.Extractor_Cellphones;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CrawlerConfiguration {
    @Bean(value = "cellphones-crawler")
    public Crawler getCrawler_cellphones() {
        return new Crawler_Cellphones();
    }

    @Bean(value = "cellphones-extractor")
    public Extractor getExtractor_cellphones() {
        return new Extractor_Cellphones();
    }
}
