package com.mydb.mydb.service;

import com.mydb.mydb.Config;
import com.mydb.mydb.entity.Payload;
import com.mydb.mydb.entity.SegmentIndex;
import com.mydb.mydb.exception.UnknownProbeException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@Service
public class LSMService {

  public static final long MAX_MEMTABLE_SIZE = 2;
  private final FileIOService fileIOService;
  private final SegmentService segmentService;
  private Map<String, Payload> memTable = new TreeMap<>();
  private List<SegmentIndex> indices;

  @Autowired
  public LSMService(FileIOService fileIOService, SegmentService segmentService) {
    this.fileIOService = fileIOService;
    this.segmentService = segmentService;
    indices =  new LinkedList<>();
  }

  public Payload insert(final Payload payload) throws IOException {
    memTable.put(payload.getProbeId(), payload);
    if (memTable.size() == MAX_MEMTABLE_SIZE) {
      if(indices.isEmpty())
        indices.add(fileIOService.persistConfig(segmentService.getNewSegmentPath(), memTable));
      else
        indices.add(indices.size() - 1, fileIOService.persistConfig(segmentService.getNewSegmentPath(), memTable));
      fileIOService.persistConfig(Config.CONFIG_PATH, segmentService.getCurrentSegmentConfig());
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

  private Payload getDataFromSegments(final String probeId) {

    var segmentIndex = indices.stream().filter(x -> x.getIndex().containsKey(probeId)).findFirst().orElse(null);

    var segmentPath =  segmentService.getPathForSegment(segmentIndex.getSegmentName());

    var payload =  fileIOService.getPayload(segmentPath, segmentIndex.getIndex().get(probeId));

    return payload.orElse(null);

  }
}
