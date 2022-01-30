package com.mydb.mydb;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class MydbApplication extends SpringBootServletInitializer {

  public static long MAX_MEM_TABLE_SIZE = 10;

	public static void main(String[] args) {
	  if(args.length > 0)
	    MAX_MEM_TABLE_SIZE = Long.parseLong(args[0]);
		SpringApplication.run(MydbApplication.class, args);
	}

}
