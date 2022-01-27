package com.mydb.mydb.service;

import com.mydb.mydb.Config;
import com.mydb.mydb.SegmentConfig;
import com.mydb.mydb.entity.Backup;
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
    return new SegmentConfig(segmentConfig.getBasePath(), segmentConfig.getCount(), segmentConfig.getBackupCount());
  }

  @Autowired
  public SegmentService(@Qualifier("segmentConfig") final SegmentConfig segmentConfig, FileIOService fileIOService) {
      this.segmentConfig = segmentConfig;
      this.fileIOService = fileIOService;
  }

  public synchronized Segment getNewSegment() throws IOException {
    var newCount = segmentConfig.getCount() + 1;
    var newSegmentName = getSegmentName(newCount);
    var newSegmentPath = getPathForSegment(newSegmentName);
    // TODO: Remove this updation from here and do it after persistence
    segmentConfig.setCount(newCount);
    fileIOService.persistConfig(Config.CONFIG_PATH, getCurrentSegmentConfig());
    return new Segment(newSegmentName, newSegmentPath);
  }

  public Backup getNewBackup() throws IOException {
    var newBackupName = getBackupName(segmentConfig.getBackupCount() + 1);
    var newBackupPath = getPathForBackup(newBackupName);
    return new Backup(newBackupName, newBackupPath);
  }

  public String getCurrentBackupPath() {
    return getPathForBackup(getBackupName(segmentConfig.getBackupCount()));
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

  public int getBackupCount() {
    return segmentConfig.getBackupCount();
  }

  public void setBackupCount(final int count) {
    segmentConfig.setBackupCount(count);
    fileIOService.persistConfig(Config.CONFIG_PATH, getCurrentSegmentConfig());
  }
}
