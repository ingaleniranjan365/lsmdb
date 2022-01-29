package com.mydb.mydb.service;

import com.mydb.mydb.Config;
import com.mydb.mydb.SegmentConfig;
import com.mydb.mydb.entity.Backup;
import com.mydb.mydb.entity.Segment;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.io.IOException;

import static com.mydb.mydb.Config.CONFIG_PATH;

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

  public synchronized Segment getNewSegment() {
    segmentConfig.setCount(segmentConfig.getCount() + 1);
    var newSegmentName = getSegmentName(segmentConfig.getCount());
    var newSegmentPath = getPathForSegment(newSegmentName);
    var newBackupName = getBackupName(segmentConfig.getCount());
    var newBackupPath = getPathForBackup(newSegmentName);
    persistConfig();
    return new Segment(
        newSegmentName,
        newSegmentPath,
        newBackupName,
        newBackupPath
    );
  }

  private synchronized void persistConfig() {
    fileIOService.persistConfig(CONFIG_PATH, getCurrentSegmentConfig());
  }

  private String getSegmentName(int i) {
    return String.format("segment-%d", i);
  }

  private String getBackupName(int i) {
    return String.format("backup-%d", i);
  }

  public String getPathForSegment(String segmentName) {
    return segmentConfig.getBasePath() + "/"+ segmentName ;
  }

  public String getPathForBackup(String backupName) {
    return segmentConfig.getBasePath() + "/indices/"+ backupName ;
  }

}
