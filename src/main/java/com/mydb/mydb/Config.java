package com.mydb.mydb;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mydb.mydb.entity.Index;
import com.mydb.mydb.entity.Payload;
import com.mydb.mydb.service.FileIOService;
import com.mydb.mydb.service.LSMService;
import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.SerializationUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.IntFunction;
import java.util.stream.IntStream;

@Configuration
public class Config {

  public static final String PATH_TO_REPOSITORY_ROOT =  System.getProperty("user.dir");
  public static final String CONFIG_PATH = PATH_TO_REPOSITORY_ROOT + "/src/main/resources/segmentState.json";
  public static final String DEFAULT_BASE_PATH = PATH_TO_REPOSITORY_ROOT + "/src/main/resources/segments";
  public static final String DEFAULT_WAL_FILE_PATH = PATH_TO_REPOSITORY_ROOT + "/src/main/resources/segments/wal/wal";

  private static final ObjectMapper mapper = new ObjectMapper();

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

  // TODO: test that failure/exception to deserialize a payload does not fail the whole file processing
  @Bean("memTable")
  public Map<String, Payload> fromWAL() {
    var memTable = new TreeMap<String, Payload>();
    try {
      var walFile = new File(DEFAULT_WAL_FILE_PATH);
      if(walFile.exists()) {
        byte[] wal = null;

        try {
          wal = FileUtils.readFileToByteArray(walFile);
        } catch (IOException | NullPointerException e) {
          e.printStackTrace();
        }

        if(wal!=null) {
          byte[] finalWal = wal;
          IntStream.range(0, finalWal.length/LSMService.MAX_PAYLOAD_SIZE)
              .map(i -> i*LSMService.MAX_PAYLOAD_SIZE)
              .mapToObj(i -> deserialize(finalWal, i))
              .map(this::getJsonStr)
              .forEach(jsonStr -> extractPayload(memTable, jsonStr));
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

  private Object deserialize(byte[] finalWal, int i) {
    try {
      return SerializationUtils.deserialize(Arrays.copyOfRange(finalWal, i, i + LSMService.MAX_PAYLOAD_SIZE));
    } catch (IllegalArgumentException | IllegalStateException ex) {
      ex.printStackTrace();
      return null;
    }
  }


  private void extractPayload(TreeMap<String, Payload> memTable, String jsonStr) {
    if(jsonStr !=null) {
      try {
        var payload = mapper.readValue(jsonStr, Payload.class);
        memTable.put(payload.getProbeId(), payload);
      } catch (JsonProcessingException | RuntimeException e) {
        e.printStackTrace();
      }
    }
  }

  private String getJsonStr(Object obj) {
    try {
      return mapper.writeValueAsString(obj);
    } catch (IOException | RuntimeException e) {
      e.printStackTrace();
      return null;
    }
  }

}
