package com.mydb.mydb;

import com.mydb.mydb.entity.SegmentIndex;
import com.mydb.mydb.service.FileIOService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.File;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedDeque;

@Configuration
public class Config {

  public static final String PATH_TO_REPOSITORY_ROOT =  System.getProperty("user.dir");
  public static final String CONFIG_PATH = PATH_TO_REPOSITORY_ROOT + "/src/main/resources/segmentState.json";
  public static final String DEFAULT_BASE_PATH = PATH_TO_REPOSITORY_ROOT + "/src/main/resources/segments";
  public static final String DEFAULT_INDICES_PATH = PATH_TO_REPOSITORY_ROOT + "/src/main/resources/segments/indices/backup";

  @Autowired
  private FileIOService fileIOService;

  @Bean("segmentConfig")
  public SegmentConfig getSegmentConfig() {
      var segmentConfig = fileIOService.getSegmentConfig(CONFIG_PATH);
      if(segmentConfig.isPresent()) {
        var config = segmentConfig.get();
        config.setBasePath(DEFAULT_BASE_PATH);
        return config;
      }
      return new SegmentConfig(DEFAULT_BASE_PATH, 0);
  }

  @Bean("indices")
  public ConcurrentLinkedDeque<SegmentIndex> getIndices()  {
    var indices = fileIOService.getIndices(DEFAULT_INDICES_PATH);
    return indices.orElseGet(ConcurrentLinkedDeque::new);
  }

}
