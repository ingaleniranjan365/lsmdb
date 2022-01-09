package com.mydb.mydb.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.mydb.mydb.entity.Payload;
import org.springframework.stereotype.Service;

import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;

@Service
public class PersistenceService {

  public static final ObjectMapper mapper = new ObjectMapper();

  public void persist(final String segmentPath, final Map<String, Payload> memTable) throws IOException {
    var json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(memTable);
    FileOutputStream outputStream = new FileOutputStream(segmentPath);
    outputStream.write(json.getBytes());
    outputStream.close();
  }
}
