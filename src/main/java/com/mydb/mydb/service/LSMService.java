package com.mydb.mydb.service;

import com.mydb.mydb.entity.Payload;
import com.mydb.mydb.exception.UnknownProbeException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

@Service
public class LSMService {

  public static final long MAX_MEMTABLE_SIZE = 2;
  private FileIOService fileIOService;
  private SegmentService segmentService;
  private Map<String, Payload> memTable = new TreeMap<>();

  @Autowired
  public LSMService(FileIOService fileIOService, SegmentService segmentService) {
    this.fileIOService = fileIOService;
    this.segmentService = segmentService;
  }

  public Payload insert(final Payload payload) throws IOException {
    memTable.put(payload.getProbeId(), payload);
    if(memTable.size() == MAX_MEMTABLE_SIZE) {
      fileIOService.persist(segmentService.getNewSegmentPath(), memTable);
      memTable = new TreeMap<>();
    }
    return payload;
  }

  public Payload getData(String probeId) throws UnknownProbeException {
    return Optional.ofNullable(memTable.getOrDefault(probeId, null))
        .orElse(Optional.ofNullable(getDataFromSegments(probeId))
        .orElseThrow(() -> new UnknownProbeException(String.format("Probe id - %s not found!", probeId))));
  }

  private Payload getDataFromSegments(final String probeId) throws UnknownProbeException {

    return segmentService.getAllSegmentPaths().stream().map(path ->
        fileIOService.getSegment(path))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .filter(x -> x.containsKey(probeId))
        .findFirst()
        .map(segment -> segment.get(probeId)).orElse(null);
  }
}
