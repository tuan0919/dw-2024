package com.nlu.app.configuration;

import org.openqa.selenium.chrome.ChromeDriver;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SeleniumConfiguration {
    @Bean
    public ChromeDriver getChromeDriver() {
        return new ChromeDriver();
    }
}
