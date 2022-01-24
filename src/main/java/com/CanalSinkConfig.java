package com;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@ConfigurationProperties(prefix = "sync.sink")
@Component
public class CanalSinkConfig {
    private String host;
    private int port;
    private String user;
    private String password;
    private boolean ignoreDDL=false;
}
