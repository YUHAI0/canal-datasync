package com;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;

@SpringBootApplication
@EnableConfigurationProperties
public class ApiApplication {

	public static void main(String[] args) {
		ApplicationContext ctx = SpringApplication.run(ApiApplication.class, args);
		ctx.getBean(SyncDaemon.class).run();
	}
}
