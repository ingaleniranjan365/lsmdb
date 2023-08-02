package com.lsmdb;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lsmdb.entity.SegmentIndex;
import com.lsmdb.service.FileIOService;
import io.vertx.core.buffer.Buffer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.io.File;
import java.io.IOException;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

import static com.lsmdb.service.FileIOService.DEFAULT_WAL_FILE_PATH;
import static com.lsmdb.service.FileIOService.DELIMITER;
import static com.lsmdb.service.FileIOService.STAGED_WAL_FILE_PATH;

public class StateLoader {

  // TODO: Remove duplication of conf
  public static final String PATH_TO_HOME = System.getProperty("user.home");
  public static final String CONFIG_PATH = PATH_TO_HOME + "/data/segmentState.json";
  public static final String DEFAULT_BASE_PATH = PATH_TO_HOME + "/data/segments";
  // TODO: Remove this mapper and use common mapper
  public static final ObjectMapper mapper = new ObjectMapper();

  private final FileIOService fileIOService;

  public StateLoader(final FileIOService fileIOService) {
    this.fileIOService = fileIOService;
  }

  public SegmentConfig getSegmentConfig() {
    var segmentConfig = fileIOService.getSegmentConfig(CONFIG_PATH);
    if (segmentConfig.isPresent()) {
      var config = segmentConfig.get();
      config.setBasePath(DEFAULT_BASE_PATH);
      return config;
    }
    return new SegmentConfig(DEFAULT_BASE_PATH, -1);
  }

  public Deque<SegmentIndex> getIndices() {
    var segmentConfig = fileIOService.getSegmentConfig(CONFIG_PATH);
    var indices = new ConcurrentLinkedDeque<SegmentIndex>();
    if (segmentConfig.isPresent()) {
      var counter = segmentConfig.get().getCount();
      while (counter >= 0) {
        var index = fileIOService.getIndex(DEFAULT_BASE_PATH + "/indices/backup-" + counter);
        index.ifPresent(indices::addLast);
        counter--;
      }
    }
    return indices;
  }

  public ImmutablePair<Deque<String>, Map<String, Deque<Buffer>>> getMemTableDataFromWAL() {
    var memTable = new ConcurrentHashMap<String, Deque<Buffer>>();
    var ids = new ConcurrentLinkedDeque<String>();
    List.of(STAGED_WAL_FILE_PATH, DEFAULT_BASE_PATH).forEach( walPath -> {
          try {
            var walFile = new File(DEFAULT_WAL_FILE_PATH);
            if (walFile.exists()) {
              var wal = readWAL(walFile, null);
              if (wal != null) {
                writeToMemory(ids, memTable, wal);
              }
              deleteWALFile(walFile);
            }
          } catch (RuntimeException ex) {
            ex.printStackTrace();
          }
        }
    );
    return new ImmutablePair<>(ids, memTable);
  }

  private void writeToMemory(Deque<String> ids, Map<String, Deque<Buffer>> memTable, byte[] wal) {
    String finalWal = new String(wal);
    var delimiter = new String(DELIMITER);
    String[] split = finalWal.split(delimiter);
    for (String payload : split) {
      try {
        var id = getId(payload);
        if (memTable.containsKey(id)) {
          memTable.get(id).addLast(Buffer.buffer(payload));
        } else {
          var list = new ConcurrentLinkedDeque<Buffer>();
          list.addLast(Buffer.buffer(payload));
          memTable.put(id, list);
        }
        ids.addLast(id);
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

  private String getId(String payload) throws JsonProcessingException {
    return mapper.readTree(payload).get("id").toString().replace("\"", "");
  }

  private void deleteWALFile(File walFile) {
    try {
      walFile.delete();
    } catch (SecurityException ex) {
      ex.printStackTrace();
    }
  }

}
