package com.mydb.mydb;

import com.mydb.mydb.entity.Index;
import com.mydb.mydb.entity.SegmentIndex;
import com.mydb.mydb.service.FileIOService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedDeque;

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
      if(segmentConfig.isPresent()) {
        var config = segmentConfig.get();
        config.setBasePath(DEFAULT_BASE_PATH);
        return config;
      }
      return new SegmentConfig(DEFAULT_BASE_PATH, 0, 0);
  }

  @Bean("index")
  public Index getIndex()  {
    var segmentConfig = fileIOService.getSegmentConfig(CONFIG_PATH);
    if(segmentConfig.isPresent()) {
      var index = fileIOService.getIndex(
          DEFAULT_BASE_PATH + "/indices/backup-" + segmentConfig.get().getBackupCount()
      );
      if(index.isPresent()) {
        return index.get();
      }
    }
    return new Index(null, new ConcurrentLinkedDeque<>());
  }

}
