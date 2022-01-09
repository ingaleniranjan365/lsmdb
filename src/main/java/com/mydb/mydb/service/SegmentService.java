package com.mydb.mydb.service;

import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class SegmentService {

  public static final String PATH_TO_REPOSITORY_ROOT =  System.getProperty("user.dir");


  public String getNewSegmentPath() {
    return PATH_TO_REPOSITORY_ROOT + "/src/main/resources/segments/segment-0.json";
  }

  public List<String> getAllSegmentPaths() {
    return List.of(PATH_TO_REPOSITORY_ROOT + "/src/main/resources/segments/segment-2.json",
        PATH_TO_REPOSITORY_ROOT + "/src/main/resources/segments/segment-1.json",
        PATH_TO_REPOSITORY_ROOT + "/src/main/resources/segments/segment-0.json"
        );
  }
}
