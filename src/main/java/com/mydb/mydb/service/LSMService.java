package com.mydb.mydb.service;

import com.mydb.mydb.Config;
import com.mydb.mydb.entity.Payload;
import com.mydb.mydb.exception.UnknownProbeException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

@Service
public class LSMService {

  public static final long MAX_MEMTABLE_SIZE = 2;
  private final FileIOService fileIOService;
  private final SegmentService segmentService;
  private Map<String, Payload> memTable = new TreeMap<>();

  @Autowired
  public LSMService(FileIOService fileIOService, SegmentService segmentService) {
    this.fileIOService = fileIOService;
    this.segmentService = segmentService;
  }

  public Payload insert(final Payload payload) throws IOException {
    memTable.put(payload.getProbeId(), payload);
    if (memTable.size() == MAX_MEMTABLE_SIZE) {
      fileIOService.persist(segmentService.getNewSegmentPath(), memTable);
      fileIOService.persist(Config.CONFIG_PATH, segmentService.getCurrentSegmentConfig());
      memTable = new TreeMap<>();
    }
    return payload;
  }

  public Payload getData(String probeId) throws UnknownProbeException {
    var data = memTable.getOrDefault(probeId, null);
    if (data == null) {
      var dataFromSegments = getDataFromSegments(probeId);
      if (dataFromSegments == null) {
        throw new UnknownProbeException(String.format("Probe id - %s not found!", probeId));
      }
      return dataFromSegments;
    }
    return data;
  }

  private Payload getDataFromSegments(final String probeId) throws UnknownProbeException {

    return segmentService.getAllSegmentPaths().stream().map(path ->
        { var x = fileIOService.getSegment(path, probeId);
          return fileIOService.getSegment(path);})
        .filter(Optional::isPresent)
        .map(Optional::get)
        .filter(x -> x.containsKey(probeId))
        .findFirst()
        .map(segment -> segment.get(probeId)).orElse(null);
  }
}
