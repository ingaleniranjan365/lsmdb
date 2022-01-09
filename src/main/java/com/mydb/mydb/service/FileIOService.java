package com.mydb.mydb.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.mydb.mydb.entity.Payload;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Service
public class FileIOService {

  public static final ObjectMapper mapper = new ObjectMapper();

  public void persist(final String segmentPath, final Map<String, Payload> memTable) throws IOException {
    var json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(memTable);
    FileOutputStream outputStream = new FileOutputStream(segmentPath);
    outputStream.write(json.getBytes());
    outputStream.close();
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
}
