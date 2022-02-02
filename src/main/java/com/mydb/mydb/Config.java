package com.mydb.mydb;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mydb.mydb.entity.SegmentIndex;
import com.mydb.mydb.service.FileIOService;
import com.mydb.mydb.service.LSMService;
import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.stream.IntStream;

@Configuration
public class Config {

  public static final String PATH_TO_HOME = System.getProperty("user.home");
  public static final String CONFIG_PATH = PATH_TO_HOME + "/data/segmentState.json";
  public static final String DEFAULT_BASE_PATH = PATH_TO_HOME + "/data/segments";
  public static final String DEFAULT_WAL_FILE_PATH = PATH_TO_HOME + "/data/segments/wal/wal";
  public static final ObjectMapper mapper = new ObjectMapper();

  @Autowired
  private FileIOService fileIOService;

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
  public Map<String, String> fromWAL() {
    var memTable = new TreeMap<String, String>();
    try {
      var walFile = new File(DEFAULT_WAL_FILE_PATH);
      if (walFile.exists()) {
        byte[] wal = null;

        try {
          wal = FileUtils.readFileToByteArray(walFile);
        } catch (IOException | NullPointerException e) {
          e.printStackTrace();
        }

        if (wal != null) {
          byte[] finalWal = wal;
          IntStream.range(0, finalWal.length / LSMService.MAX_PAYLOAD_SIZE)
              .map(i -> i * LSMService.MAX_PAYLOAD_SIZE)
              .mapToObj(i -> deserialize(finalWal, i))
              .forEach(payload -> {
                if (payload != null) {
                  try {
                    memTable.put(mapper.readTree(payload).get, payload);
                  } catch (RuntimeException | JsonProcessingException e) {
                    e.printStackTrace();
                  }
                }
              });
        }

        try {
          walFile.delete();
        } catch (SecurityException ex) {
          ex.printStackTrace();
        }

      }
    } catch (RuntimeException ex) {
      ex.printStackTrace();
    }
    return memTable;
  }

  private String deserialize(byte[] finalWal, int i) {
    try {
      var in = Arrays.copyOfRange(finalWal, i, i + LSMService.MAX_PAYLOAD_SIZE);
      ByteArrayInputStream bis = new ByteArrayInputStream(in);
      ObjectInputStream ois = new ObjectInputStream(bis);
      return (String) ois.readObject();
    } catch (ClassNotFoundException | IOException | RuntimeException ex) {
      ex.printStackTrace();
      return null;
    }
  }
}
