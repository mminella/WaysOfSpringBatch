package io.spring;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;

@ComponentScan
@EnableAutoConfiguration
public class Application {

	public static void main(String[] args) {
		args = new String[2];
		args[0] = "inputFile=/tmp/logs_temp/swk_small.log";
		args[1] = "stagingDirectory=/tmp/logs_temp/out/";
		SpringApplication.run(Application.class, args);
	}
}
