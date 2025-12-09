package com.viettel.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@ConfigurationProperties("pm.system-codes")
@Configuration
@Data
public class LoadingSystemCodeProperties {
    private Map<String, String> mapping = new HashMap<>();
}
