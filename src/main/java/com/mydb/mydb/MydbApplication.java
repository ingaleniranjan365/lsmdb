package com.mydb.mydb;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.io.File;

@EnableScheduling
@SpringBootApplication
public class MydbApplication {

  public static int MAX_MEM_TABLE_SIZE = 1000;

  public static void main(String[] args) {
    if (args.length > 0)
      MAX_MEM_TABLE_SIZE = Integer.parseInt(args[0]);
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
