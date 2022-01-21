package com.mydb.mydb.service;

import com.mydb.mydb.Config;
import com.mydb.mydb.SegmentConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class SegmentService {

  private final SegmentConfig segmentConfig;
  private final FileIOService fileIOService;

  public SegmentConfig getCurrentSegmentConfig() {
    return new SegmentConfig(segmentConfig.getBasePath(), segmentConfig.getCount());
  }

  @Autowired
  public SegmentService(@Qualifier("segmentConfig") final SegmentConfig segmentConfig, FileIOService fileIOService) {
      this.segmentConfig = segmentConfig;
      this.fileIOService = fileIOService;
  }

  public String getNewSegmentPath() throws IOException {
    var nextPath = getPathForSegment(segmentConfig.getCount());
    segmentConfig.setCount(segmentConfig.getCount() + 1);
    fileIOService.persistConfig(Config.CONFIG_PATH, getCurrentSegmentConfig());
    return nextPath;
  }

  private String getPathForSegment(Integer i) {
    return segmentConfig.getBasePath() + String.format("/segment-%d", i);
  }

  public String getPathForSegment(String segmentName) {
    return segmentConfig.getBasePath() + "/"+ segmentName ;
  }

}
