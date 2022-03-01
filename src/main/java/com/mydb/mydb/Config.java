package com.mydb.mydb;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mydb.mydb.entity.SegmentIndex;
import com.mydb.mydb.service.FileIOService;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.sleuth.instrument.async.LazyTraceExecutor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.File;
import java.io.IOException;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

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

  @Value("${config.threadPoolSize}")
  private int nThreads;

  @Bean("fixedThreadPool")
  public Executor fixedThreadPool(final BeanFactory beanFactory) {
    var executor = Executors.newFixedThreadPool(nThreads);
    return new LazyTraceExecutor(beanFactory, executor);
  }

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
  public Deque<SegmentIndex> getIndices() {
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

  @Bean("memTableData")
  public ImmutablePair<Deque<String>, Map<String, Deque<String>>> getMemTableDataFromWAL() {
    var memTable = new ConcurrentHashMap<String, Deque<String>>();
    var probeIds = new ConcurrentLinkedDeque<String>();
    try {
      var walFile = new File(DEFAULT_WAL_FILE_PATH);
      if (walFile.exists()) {
        var wal = readWAL(walFile, null);
        if (wal != null) {
          writeToMemory(probeIds, memTable, wal);
        }
        deleteWALFile(walFile);
      }
    } catch (RuntimeException ex) {
      ex.printStackTrace();
    }
    return new ImmutablePair<>(probeIds, memTable);
  }

  private void writeToMemory(Deque<String> probeIds, Map<String, Deque<String>> memTable, byte[] wal) {
    String finalWal = new String(wal);
    String[] split = finalWal.split(DELIMITER);
    for (String payload : split) {
      try {
        var probeId = getProbeId(payload);
        if (memTable.containsKey(probeId)) {
          memTable.get(probeId).addLast(payload);
        } else {
          var list = new ConcurrentLinkedDeque<String>();
          list.addLast(payload);
          memTable.put(probeId, list);
        }
        probeIds.addLast(probeId);
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
