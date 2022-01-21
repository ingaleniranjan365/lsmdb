package com.mydb.mydb;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class MydbApplication {

	public static void main(String[] args) {
		SpringApplication.run(MydbApplication.class, args);
	}

}
