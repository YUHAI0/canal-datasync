package com;


import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@ConfigurationProperties(prefix = "sync.canal")
@Component
public class CanalConfig {
    private String host;
    private int port;
    private String dest;
    private String user;
    private String password;
    private String filterRegex;
}
