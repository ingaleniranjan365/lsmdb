package com.mydb.mydb.service;

import com.mydb.mydb.SegmentConfig;
import com.mydb.mydb.entity.Segment;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import static com.mydb.mydb.Config.CONFIG_PATH;

@Service
public class SegmentService {

  private final SegmentConfig segmentConfig;
  private final FileIOService fileIOService;

  @Autowired
  public SegmentService(@Qualifier("segmentConfig") final SegmentConfig segmentConfig, FileIOService fileIOService) {
    this.segmentConfig = segmentConfig;
    this.fileIOService = fileIOService;
  }

  private SegmentConfig getCurrentSegmentConfig() {
    return new SegmentConfig(segmentConfig.getBasePath(), segmentConfig.getCount());
  }

  public Segment getNewSegment() {
    segmentConfig.setCount(segmentConfig.getCount() + 1);
    var newSegmentName = getSegmentName(segmentConfig.getCount());
    var newSegmentPath = getPathForSegment(newSegmentName);
    var newBackupName = getBackupName(segmentConfig.getCount());
    var newBackupPath = getPathForBackup(newBackupName);
    fileIOService.persistConfig(CONFIG_PATH, getCurrentSegmentConfig());
    return new Segment(
        newSegmentName,
        newSegmentPath,
        newBackupName,
        newBackupPath
    );
  }

  private String getSegmentName(long i) {
    return String.format("segment-%d", i);
  }

  private String getBackupName(long i) {
    return String.format("backup-%d", i);
  }

  public String getPathForSegment(String segmentName) {
    return segmentConfig.getBasePath() + "/" + segmentName;
  }

  public String getPathForBackup(String backupName) {
    return segmentConfig.getBasePath() + "/indices/" + backupName;
  }

}
