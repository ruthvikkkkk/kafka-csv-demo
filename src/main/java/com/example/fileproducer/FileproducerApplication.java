package com.example.fileproducer;

import com.example.fileproducer.service.FileStorageService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

import java.util.Properties;

@SpringBootApplication
@EnableSwagger2
@EnableJpaRepositories("com.example.repository")
@EntityScan("com.example.entities")
@ComponentScan(basePackages = {"com.example.repository", "com.example.fileproducer"})
public class FileproducerApplication implements CommandLineRunner {
	@Autowired
	FileStorageService storageService;

	public static void main(String[] args) {

		SpringApplication application = new SpringApplication(FileproducerApplication.class);

		Properties properties = new Properties();
		properties.setProperty("server.port", "8080");
		application.setDefaultProperties(properties);

		application.run(args);
//		SpringApplication.run(FileproducerApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		storageService.init();
	}
}
