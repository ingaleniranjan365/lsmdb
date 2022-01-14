package com.mydb.mydb;

import com.mydb.mydb.service.FileIOService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class Config {

  public static final String PATH_TO_REPOSITORY_ROOT =  System.getProperty("user.dir");
  public static final String CONFIG_PATH = PATH_TO_REPOSITORY_ROOT + "/src/main/resources/segmentState.json";
  public static final String DEFAULT_BASE_PATH = PATH_TO_REPOSITORY_ROOT + "/src/main/resources/segments";

  @Autowired
  private FileIOService fileIOService;

  @Bean("segmentConfig")
  public SegmentConfig getSegmentConfig() {
      var segmentConfig = fileIOService.getSegmentConfig(CONFIG_PATH);
    return segmentConfig.orElseGet(() -> new SegmentConfig(DEFAULT_BASE_PATH, 0));
  }
}
