package com.nlu.app.pojo;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@ConfigurationProperties(prefix = "config")
public class ApplicationProcessConfig {
    private List<ProcessDetail> process;

    public List<ProcessDetail> getProcess() {
        return process;
    }

    public void setProcess(List<ProcessDetail> process) {
        this.process = process;
    }

    public static class ProcessDetail {
        private String configName;
        private Integer configId;

        public String getConfigName() {
            return configName;
        }

        public void setConfigName(String configName) {
            this.configName = configName;
        }

        public Integer getConfigId() {
            return configId;
        }

        public void setConfigId(Integer configId) {
            this.configId = configId;
        }
    }
}
