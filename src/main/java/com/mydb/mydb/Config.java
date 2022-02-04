package com.mydb.mydb;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mydb.mydb.entity.SegmentIndex;
import com.mydb.mydb.service.FileIOService;
import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;

import static com.mydb.mydb.service.FileIOService.DEFAULT_WAL_FILE_PATH;
import static com.mydb.mydb.service.FileIOService.DELIMITER;

@Configuration
public class Config {

  public static final String PATH_TO_HOME = System.getProperty("user.home");
  public static final String CONFIG_PATH = PATH_TO_HOME + "/data/segmentState.json";
  public static final String DEFAULT_BASE_PATH = PATH_TO_HOME + "/data/segments";
  public static final ObjectMapper mapper = new ObjectMapper();

  @Autowired
  private FileIOService fileIOService;

  @Bean("mapper")
  public ObjectMapper mapper() {
    return mapper;
  }

  @Bean("segmentConfig")
  public SegmentConfig getSegmentConfig() {
    var segmentConfig = fileIOService.getSegmentConfig(CONFIG_PATH);
    if (segmentConfig.isPresent()) {
      var config = segmentConfig.get();
      config.setBasePath(DEFAULT_BASE_PATH);
      return config;
    }
    return new SegmentConfig(DEFAULT_BASE_PATH, -1);
  }

  @Bean("indices")
  public ConcurrentLinkedDeque<SegmentIndex> getIndices() {
    var segmentConfig = fileIOService.getSegmentConfig(CONFIG_PATH);
    if (segmentConfig.isPresent()) {
      var counter = segmentConfig.get().getCount();
      while (counter >= 0) {
        var index = fileIOService.getIndices(
            DEFAULT_BASE_PATH + "/indices/backup-" + counter);
        if (index.isPresent()) {
          return index.get();
        }
        counter--;
      }
    }
    return new ConcurrentLinkedDeque<>();
  }

  @Bean("memTable")
  public Map<String, String> getMemTableFromWAL() {
    var memTable = new LinkedHashMap<String , String>();
    try {
      var walFile = new File(DEFAULT_WAL_FILE_PATH);
      if (walFile.exists()) {
        var wal = readWAL(walFile, null);
        if (wal != null) {
          writeToMemory(memTable, wal);
        }
        deleteWALFile(walFile);
      }
    } catch (RuntimeException ex) {
      ex.printStackTrace();
    }
    return memTable;
  }

  private void writeToMemory(Map<String, String> memTable, byte[] wal) {
    String finalWal = new String(wal);
    String[] split = finalWal.split(DELIMITER);
    for (String payload : split) {
      try {
        memTable.put(getProbeId(payload), payload);
      } catch (JsonProcessingException exception) {
        exception.printStackTrace();
      }
    }
  }

  private byte[] readWAL(File walFile, byte[] wal) {
    try {
      wal = FileUtils.readFileToByteArray(walFile);
    } catch (IOException | NullPointerException e) {
      e.printStackTrace();
    }
    return wal;
  }

  private String getProbeId(String payload) throws JsonProcessingException {
    return mapper.readTree(payload).get("probeId").toString().replace("\"", "");
  }

  private void deleteWALFile(File walFile) {
    try {
      walFile.delete();
    } catch (SecurityException ex) {
      ex.printStackTrace();
    }
  }

}
