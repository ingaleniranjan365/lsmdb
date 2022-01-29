package com.mydb.mydb.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mydb.mydb.SegmentConfig;
import com.mydb.mydb.entity.Payload;
import com.mydb.mydb.entity.Segment;
import com.mydb.mydb.entity.SegmentIndex;
import com.mydb.mydb.entity.SegmentMetadata;
import org.apache.commons.io.FileUtils;
import org.springframework.stereotype.Service;
import org.springframework.util.SerializationUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedDeque;

@Service
public class FileIOService {

  public static final ObjectMapper mapper = new ObjectMapper();


  public SegmentIndex persist(final Segment segment, final Map<String, Payload> memTable) {
    final Map<String, SegmentMetadata> index = new LinkedHashMap<>();
    File segmentFile = new File(segment.getSegmentPath());
    memTable.forEach((key, value) -> {
      try {
      var bytes = SerializationUtils.serialize(value);
      index.put(key, new SegmentMetadata((int) (segmentFile.length()), bytes.length));
      FileUtils.writeByteArrayToFile(segmentFile, bytes, true);
      } catch (RuntimeException | IOException ex) {
        ex.printStackTrace();
      }
    });

    return new SegmentIndex(segment, index);
  }

  public void persistConfig(final String configPath, final SegmentConfig config)  {
    try {
      var json = mapper.writeValueAsString(config);
      FileOutputStream outputStream = new FileOutputStream(configPath);
      outputStream.write(json.getBytes());
      outputStream.close();
    } catch (IOException exception) {
      exception.printStackTrace();
    }
  }

  public Optional<Payload> getPayload(final String path, final SegmentMetadata metadata) {
    try {
      var in = readBytes(path, metadata);
      // TODO: Figure out a faster way to deserialize than to first mapping
      //  bytes-> obj, obj -> jsonStr and then jsonStr -> Payload
      var obj = SerializationUtils.deserialize(in);
      var jsonStr = mapper.writeValueAsString(obj);
      return Optional.of(mapper.readValue(jsonStr, Payload.class));
    } catch (IOException e) {
      e.printStackTrace();
      return Optional.empty();
    }
  }

  public byte[] readBytes(final String path, final SegmentMetadata metadata) throws IOException {
    RandomAccessFile raf = null;
    raf = new RandomAccessFile(path, "r");
    raf.seek((long) metadata.getOffset());
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

  public Optional<ConcurrentLinkedDeque<SegmentIndex>> getIndices(String path){
    try {
      File file = new File(path);
      var in = FileUtils.readFileToByteArray(file);
      // TODO: Figure out a faster way to deserialize than to first mapping
      //  bytes-> obj, obj -> jsonStr and then jsonStr -> Payload
      var obj = SerializationUtils.deserialize(in);
      var jsonStr = mapper.writeValueAsString(obj);
      var indices = mapper.readValue(jsonStr, new TypeReference<ConcurrentLinkedDeque<SegmentIndex>>() {});
      return Optional.of(indices);
    } catch (IOException e) {
      e.printStackTrace();
      return Optional.empty();
    }
  }


  public void persistIndices(final String newBackupPath, final byte[] indicesBytes) {
    try {
      var newIndexFile = new File(newBackupPath);
      FileUtils.writeByteArrayToFile(newIndexFile, indicesBytes);
    } catch (IOException | RuntimeException ex) {
      ex.printStackTrace();
    }
  }

}
