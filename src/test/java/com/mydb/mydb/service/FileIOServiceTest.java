package com.mydb.mydb.service;

import com.mydb.mydb.entity.Payload;
import org.junit.jupiter.api.Test;
import org.springframework.util.SerializationUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

class FileIOServiceTest {

  @Test
  public void readBinaryFile() throws IOException {
    File file = new File("/Users/niranjani/code/big-o/mydb/src/main/resources/segments/binary_file");
    byte[] fileContent = Files.readAllBytes(file.toPath());
    Payload payload = (Payload) SerializationUtils.deserialize(fileContent);

    assert fileContent.length == 100;
  }

}
