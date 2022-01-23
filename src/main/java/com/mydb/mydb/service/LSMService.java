package com.mydb.mydb.service;

import com.mydb.mydb.entity.Payload;
import com.mydb.mydb.entity.SegmentIndex;
import com.mydb.mydb.exception.UnknownProbeException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedDeque;

@Slf4j
@Service
public class LSMService {

  public static final long MAX_MEM_TABLE_SIZE = 8;
  private final FileIOService fileIOService;
  private final SegmentService segmentService;
  private final MergeService mergeService;

  private ConcurrentLinkedDeque<SegmentIndex> indices;
  private Map<String, Payload> memTable = new TreeMap<>();

  @Autowired
  public LSMService(FileIOService fileIOService, SegmentService segmentService, MergeService mergeService) {
    this.fileIOService = fileIOService;
    this.segmentService = segmentService;
    this.mergeService = mergeService;
    indices = new ConcurrentLinkedDeque<>();
  }

  /**
   * TODO: 1. Make sure indices.add is executed thread safe
   *       2. Make sure indices is thread safe object.
   */
  @Scheduled(fixedRate = 10000)
  public void merge() throws IOException {
    log.info("**************\nStarting scheduled merging!\n******************");
    var mergeSegment = segmentService.getNewSegment();
    var mergedSegmentIndex = mergeService.merge(getSegmentIndexEnumeration(), mergeSegment.getSegmentPath());
    if(!mergedSegmentIndex.isEmpty()) {
      var newIndices = new ConcurrentLinkedDeque<SegmentIndex>();
      newIndices.addFirst(new SegmentIndex(mergeSegment.getSegmentName(), mergedSegmentIndex));
      indices = newIndices;
    }
  }


  public List<ImmutablePair<Enumeration<String>, SegmentIndex>> getSegmentIndexEnumeration() {
    return indices.stream()
        .map(index -> ImmutablePair.of(Collections.enumeration(index.getIndex().keySet()), index)).toList();
  }

  public Payload insert(final Payload payload) throws IOException {
    memTable.put(payload.getProbeId(), payload);
    if (memTable.size() == MAX_MEM_TABLE_SIZE) {
      indices.addFirst(fileIOService.persist(segmentService.getNewSegment().getSegmentPath(), memTable));
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
    return Optional.ofNullable(segmentIndex)
        .map(i -> segmentService.getPathForSegment(i.getSegmentName()))
        .map(p -> fileIOService.getPayload(p, segmentIndex.getIndex().get(probeId)))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .orElse(null);
  }
}
