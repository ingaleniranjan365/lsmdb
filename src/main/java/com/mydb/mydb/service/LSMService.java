package com.mydb.mydb.service;

import com.mydb.mydb.Config;
import com.mydb.mydb.entity.merge.HeapElement;
import com.mydb.mydb.entity.Payload;
import com.mydb.mydb.entity.SegmentIndex;
import com.mydb.mydb.entity.SegmentMetadata;
import com.mydb.mydb.exception.UnknownProbeException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static com.mydb.mydb.entity.merge.HeapElement.getHeapElementComparator;
import static com.mydb.mydb.entity.merge.HeapElement.isProbeIdPresentInList;

@Service
public class LSMService {

  public static final long MAX_MEMTABLE_SIZE = 8;
  private final FileIOService fileIOService;
  private final SegmentService segmentService;
  private final MergeService mergeService;
  private final List<SegmentIndex> indices;
  private Map<String, Payload> memTable = new TreeMap<>();

  @Autowired
  public LSMService(FileIOService fileIOService, SegmentService segmentService, MergeService mergeService) {
    this.fileIOService = fileIOService;
    this.segmentService = segmentService;
    this.mergeService = mergeService;
    indices = new LinkedList<>();
  }

  public Payload insert(final Payload payload) throws IOException {
    memTable.put(payload.getProbeId(), payload);
    if (memTable.size() == MAX_MEMTABLE_SIZE) {
      indices.add(0, fileIOService.persist(segmentService.getNewSegmentPath(), memTable));
      fileIOService.persistConfig(Config.CONFIG_PATH, segmentService.getCurrentSegmentConfig());
      memTable = new TreeMap<>();
    }
    return payload;
  }

  public List<Payload> merge() throws IOException {
    var segmentIndexEnumeration = indices.stream()
        .map(index -> ImmutablePair.of(Collections.enumeration(index.getIndex().keySet()), index)).toList();
    return mergeService.merge(segmentIndexEnumeration);
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
