package com.mydb.mydb;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.io.File;

@EnableScheduling
@SpringBootApplication
public class MydbApplication {

  public static void main(String[] args) {
    makeDirs();
    SpringApplication.run(MydbApplication.class, args);
  }

  private static void makeDirs() {
    try {
      new File(System.getProperty("user.home") + "/data/segments").mkdirs();
      new File(System.getProperty("user.home") + "/data/segments/indices").mkdirs();
      new File(System.getProperty("user.home") + "/data/segments/wal").mkdirs();
    } catch (RuntimeException ex) {
      ex.printStackTrace();
    }
  }

}
