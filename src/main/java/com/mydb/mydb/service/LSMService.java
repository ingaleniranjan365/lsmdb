package com.mydb.mydb.service;

import com.mydb.mydb.entity.Payload;
import com.mydb.mydb.entity.SegmentIndex;
import com.mydb.mydb.exception.UnknownProbeException;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@Service
@Getter
@Setter
public class LSMService {

  public static final long MAX_MEMTABLE_SIZE = 8;
  private final FileIOService fileIOService;
  private final SegmentService segmentService;

  private List<SegmentIndex> indices;
  private Map<String, Payload> memTable = new TreeMap<>();

  @Autowired
  public LSMService(FileIOService fileIOService, SegmentService segmentService) {
    this.fileIOService = fileIOService;
    this.segmentService = segmentService;
    indices = new LinkedList<>();
  }

  public Payload insert(final Payload payload) throws IOException {
    memTable.put(payload.getProbeId(), payload);
    if (memTable.size() == MAX_MEMTABLE_SIZE) {
      indices.add(0, fileIOService.persist(segmentService.getNewSegmentPath(), memTable));
      memTable = new TreeMap<>();
    }
    return payload;
  }


  public Payload getData(String probeId) throws UnknownProbeException {
    System.out.println();


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

    var segmentPath = segmentService.getPathForSegment(segmentIndex.getSegmentName());

    var payload = fileIOService.getPayload(segmentPath, segmentIndex.getIndex().get(probeId));

    return payload.orElse(null);

  }
}
