package com.mydb.mydb.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mydb.mydb.SegmentConfig;
import com.mydb.mydb.entity.Payload;
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

@Service
public class FileIOService {

  public static final ObjectMapper mapper = new ObjectMapper();

  public SegmentIndex persist(final String segmentPath, final Map<String, Payload> memTable) {
    final Map<String, SegmentMetadata> index = new LinkedHashMap<>();
    File segment = new File(segmentPath);
    //TODO: 1. Figure out how to write memTable to file without iterating
    memTable.forEach((key, value) -> {
      var bytes = SerializationUtils.serialize(value);
      index.put(key, new SegmentMetadata((int) (segment.length()), bytes.length));
      try {
        FileUtils.writeByteArrayToFile(segment, bytes, true);
      } catch (IOException ex) {
        throw new RuntimeException(ex.getMessage());
      }
    });
    var pathSplit =  segmentPath.split("/");
    var segmentName =  pathSplit [pathSplit.length - 1];

    return new SegmentIndex(segmentName, index);
  }

  public void persistConfig(final String configPath, final SegmentConfig config) throws IOException {
    var json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(config);
    FileOutputStream outputStream = new FileOutputStream(configPath);
    outputStream.write(json.getBytes());
    outputStream.close();
  }

  public Optional<Payload> getPayload(final String path, final SegmentMetadata metadata) {
    try {
      var in = readBytes(path, metadata);
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
    byte[] in = new byte[metadata.getSize()];
    raf.read(in, 0, metadata.getSize());
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

}
