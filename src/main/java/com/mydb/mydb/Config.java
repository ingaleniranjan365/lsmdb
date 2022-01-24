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
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedDeque;
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

  @Bean("memTable")
  public Map<String, Payload> fromWAL() {
    var memTable = new TreeMap<String, Payload>();
    try {
      var walFile = new File(DEFAULT_WAL_FILE_PATH);
      var wal = FileUtils.readFileToByteArray(walFile);
      IntStream.range(0, wal.length/LSMService.MAX_PAYLOAD_SIZE)
          .map(i -> i*LSMService.MAX_PAYLOAD_SIZE)
          .mapToObj(start -> SerializationUtils.deserialize(Arrays.copyOfRange(wal, start,
              start + LSMService.MAX_PAYLOAD_SIZE)))
          .map(this::getJsonStr)
          .forEach(jsonStr -> extractPayload(memTable, jsonStr));
      walFile.delete();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return memTable;
  }

  private void extractPayload(TreeMap<String, Payload> memTable, String jsonStr) {
    if(jsonStr !=null) {
      try {
        var payload = mapper.readValue(jsonStr, Payload.class);
        memTable.put(payload.getProbeId(), payload);
      } catch (JsonProcessingException e) {
        e.printStackTrace();
      }
    }
  }

  private String getJsonStr(Object obj) {
    try {
      return mapper.writeValueAsString(obj);
    } catch (JsonProcessingException e) {
      e.printStackTrace();
      return null;
    }
  }

}
