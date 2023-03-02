package com.example.fileconsumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

import java.util.Properties;

@SpringBootApplication
@EntityScan("com.example.entities")
@EnableJpaRepositories("com.example.repository")
@ComponentScan(basePackages = {"com.example.repository", "com.example.fileconsumer"})
public class FileconsumerApplication {

	public static void main(String[] args) {

		SpringApplication application = new SpringApplication(FileconsumerApplication.class);

		Properties properties = new Properties();
		properties.setProperty("server.port", "8081");
		application.setDefaultProperties(properties);

		application.run(args);
		//SpringApplication.run(FileconsumerApplication.class, args);
	}

}
