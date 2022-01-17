package com.mydb.mydb.service;

import com.mydb.mydb.SegmentConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service
public class SegmentService {

  private final SegmentConfig segmentConfig;

  public SegmentConfig getCurrentSegmentConfig() {
    return new SegmentConfig(segmentConfig.getBasePath(), segmentConfig.getCount());
  }

  @Autowired
  public SegmentService(@Qualifier("segmentConfig") final SegmentConfig segmentConfig) {
      this.segmentConfig = segmentConfig;
  }

  public String getNewSegmentPath() {
    var nextPath = getPathForSegment(segmentConfig.getCount());
    segmentConfig.setCount(segmentConfig.getCount() + 1);
    return nextPath;
  }

  private String getPathForSegment(Integer i) {
    return segmentConfig.getBasePath() + String.format("/segment-%d", i);
  }

  public String getPathForSegment(String segmentName) {
    return segmentConfig.getBasePath() + "/"+ segmentName ;
  }

}
