package com.mydb.mydb.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mydb.mydb.SegmentConfig;
import com.mydb.mydb.entity.Segment;
import com.mydb.mydb.entity.SegmentIndex;
import com.mydb.mydb.entity.SegmentMetadata;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executor;

import static java.util.concurrent.CompletableFuture.supplyAsync;

@Service
@Slf4j
public class FileIOService {

  public static final String PATH_TO_HOME = System.getProperty("user.home");
  public static final String DEFAULT_WAL_FILE_PATH = PATH_TO_HOME + "/data/segments/wal/wal";
  public static final File WAL_FILE = new File(DEFAULT_WAL_FILE_PATH);
  public static final String DELIMITER = "Sailee";
  public static final ObjectMapper mapper = new ObjectMapper();

  public SegmentIndex persist(
      final Segment segment,
      final Deque<String> probeIds,
      final Map<String, Deque<String>> memTable,
      final ImmutablePair<Integer, Integer> range
  ) {
    final Map<String, SegmentMetadata> index = new LinkedHashMap<>();
    File segmentFile = new File(segment.getSegmentPath());
    probeIds.stream().toList().subList(range.left, range.right + 1).stream().sorted()
        .forEach(p -> {
          try {
            var list = memTable.getOrDefault(p, null);
            if (list!=null && !list.isEmpty()) {
              var bytes = list.removeFirst().getBytes(StandardCharsets.UTF_8);
              index.put(p, new SegmentMetadata(segmentFile.length(), bytes.length));
              FileUtils.writeByteArrayToFile(segmentFile, bytes, true);
            }
          } catch (RuntimeException | IOException ex) {
            ex.printStackTrace();
          }
        });
    return new SegmentIndex(segment, index);
  }

  public void persistConfig(final String configPath, final SegmentConfig config) {
    try {
      var json = mapper.writeValueAsString(config);
      FileOutputStream outputStream = new FileOutputStream(configPath);
      outputStream.write(json.getBytes());
      outputStream.close();
    } catch (IOException exception) {
      exception.printStackTrace();
    }
  }

  public byte[] readBytes(final String path, final SegmentMetadata metadata) throws IOException {
    RandomAccessFile raf = null;
    raf = new RandomAccessFile(path, "r");
    raf.seek(metadata.getOffset());
    byte[] in = new byte[(int) metadata.getSize()];
    raf.read(in, 0, (int) metadata.getSize());
    raf.close();
    return in;
  }

  public Optional<SegmentConfig> getSegmentConfig(final String path) {
    try {
      return Optional.of(
          mapper.readValue(new File(path), SegmentConfig.class));
    } catch (IOException e) {
      e.printStackTrace();
      return Optional.empty();
    }
  }

  public Optional<Deque<SegmentIndex>> getIndices(String path) {
    try {
      File file = new File(path);
      var in = FileUtils.readFileToByteArray(file);
      ByteArrayInputStream bis = new ByteArrayInputStream(in);
      ObjectInputStream ois = new ObjectInputStream(bis);
      return Optional.of((ConcurrentLinkedDeque<SegmentIndex>) ois.readObject());
    } catch (IOException | ClassNotFoundException e) {
      e.printStackTrace();
      return Optional.empty();
    }
  }

  public Optional<String> getPayload(final String path, final SegmentMetadata metadata) {
    try {
      var in = readBytes(path, metadata);
      return Optional.of(new String(in));
    } catch (IOException e) {
      e.printStackTrace();
      return Optional.empty();
    }
  }

  public boolean persistIndices(final String newBackupPath, final byte[] indicesBytes) {
    try {
      var newIndexFile = new File(newBackupPath);
      FileUtils.writeByteArrayToFile(newIndexFile, indicesBytes);
    } catch (IOException | RuntimeException ex) {
      ex.printStackTrace();
    }
    return true;
  }

  public CompletableFuture<Boolean> writeAheadLog(String payload) {
    var bytes = (payload + DELIMITER).getBytes(StandardCharsets.UTF_8);
    return supplyAsync(() -> write(bytes));
  }

  public CompletableFuture<Boolean> writeAheadLog(String payload, Executor executor) {
    var bytes = (payload + DELIMITER).getBytes(StandardCharsets.UTF_8);
    return supplyAsync(() -> write(bytes), executor);
  }

  private Boolean write(byte[] bytes) {
    try {
      FileUtils.writeByteArrayToFile(WAL_FILE, bytes, true);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return true;
  }

}
