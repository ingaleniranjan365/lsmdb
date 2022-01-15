package com.mydb.mydb.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mydb.mydb.SegmentConfig;
import com.mydb.mydb.entity.Payload;
import com.mydb.mydb.entity.SegmentMetadata;
import org.apache.commons.io.FileUtils;
import org.springframework.stereotype.Service;
import org.springframework.util.SerializationUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Service
public class FileIOService {

  public static final ObjectMapper mapper = new ObjectMapper();
  public final Map<String, SegmentMetadata> segmentMetadataMap = new HashMap<>();

  public void persist(final String segmentPath, final Map<String, Payload> memTable) throws IOException {
    var json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(memTable);
    FileOutputStream outputStream = new FileOutputStream(segmentPath);
    outputStream.write(json.getBytes());
    outputStream.close();
    persist(memTable);
  }

  public void persist(final String configPath, final SegmentConfig config) throws IOException {
    var json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(config);
    FileOutputStream outputStream = new FileOutputStream(configPath);
    outputStream.write(json.getBytes());
    outputStream.close();
  }

  private void persist(final Map<String, Payload> memTable) throws IOException {
    File someBinaryFile = new File("/Users/niranjani/code/big-o/mydb/src/main/resources/segments" +
        "/binary_file");
    memTable.forEach((key, value) -> {
      try {
        var bytes = SerializationUtils.serialize(value);
        segmentMetadataMap.put(key, new SegmentMetadata((int) (someBinaryFile.length()), bytes.length));
        FileUtils.writeByteArrayToFile(someBinaryFile, bytes, true);
      } catch (IOException e) {
        e.printStackTrace();
      }
    });
  }

  public Optional<Map<String, Payload>> getSegment(final String path) {
    try {
      return Optional.of(
          mapper.readValue(new File(path), new TypeReference<Map<String, Payload>>() {
          }));
    } catch (IOException e) {
      e.printStackTrace();
      return Optional.empty();
    }
  }

  public Optional<Map<String, Payload>> getSegment(final String path, String probeId) {
    RandomAccessFile raf = null;
    try {
      raf = new RandomAccessFile("/Users/niranjani/code/big-o/mydb/src/main/resources/segments" +
          "/binary_file", "r");
      var metadata = segmentMetadataMap.get(probeId);
      raf.seek((long) metadata.getOffset());
      byte[] in = new byte[metadata.getSize()];
      raf.read(in, 0, metadata.getSize());
      var obj = SerializationUtils.deserialize(in);
      var jsonStr = mapper.writeValueAsString(obj);
      var payload = mapper.readValue(jsonStr, Payload.class);
    } catch (IOException e) {
      e.printStackTrace();
    }

    try {
      return Optional.of(
          mapper.readValue(new File(path), new TypeReference<Map<String, Payload>>() {
          }));
    } catch (IOException e) {
      e.printStackTrace();
      return Optional.empty();
    }
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

}
