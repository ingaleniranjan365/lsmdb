package com.mydb.mydb.service;

import com.mydb.mydb.Config;
import com.mydb.mydb.SegmentConfig;
import com.mydb.mydb.entity.Segment;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class SegmentService {

  private final SegmentConfig segmentConfig;
  private final FileIOService fileIOService;

  private SegmentConfig getCurrentSegmentConfig() {
    return new SegmentConfig(segmentConfig.getBasePath(), segmentConfig.getCount());
  }

  @Autowired
  public SegmentService(@Qualifier("segmentConfig") final SegmentConfig segmentConfig, FileIOService fileIOService) {
      this.segmentConfig = segmentConfig;
      this.fileIOService = fileIOService;
  }

  public Segment getNewSegment() throws IOException {
    var newCount = segmentConfig.getCount() + 1;
    var newSegmentName = getNewSegmentName(newCount);
    var newSegmentPath = getPathForSegment(newSegmentName);
    segmentConfig.setCount(newCount);
    fileIOService.persistConfig(Config.CONFIG_PATH, getCurrentSegmentConfig());
    return new Segment(newSegmentName, newSegmentPath);
  }

  private String getNewSegmentName(int i) {
    return String.format("segment-%d", i);
  }

  public String getPathForSegment(String segmentName) {
    return segmentConfig.getBasePath() + "/"+ segmentName ;
  }

}
