package com.mydb.mydb.service;

import com.mydb.mydb.Config;
import com.mydb.mydb.entity.Element;
import com.mydb.mydb.entity.Payload;
import com.mydb.mydb.entity.SegmentIndex;
import com.mydb.mydb.exception.UnknownProbeException;
import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.TreeMap;
import java.util.stream.IntStream;

@Service
public class LSMService {

  public static final long MAX_MEMTABLE_SIZE = 8;
  private final FileIOService fileIOService;
  private final SegmentService segmentService;
  private final List<SegmentIndex> indices;
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
      fileIOService.persistConfig(Config.CONFIG_PATH, segmentService.getCurrentSegmentConfig());
      memTable = new TreeMap<>();
    }
    return payload;
  }

  public void merge() throws IOException {
    var mergedSegment = new File("/Users/niranjani/code/big-o/mydb/src/main/resources/segments/mergedSegment");
    var heap = new PriorityQueue<Element>((e1, e2) -> {
      int probeComparison = e1.getProbeId().compareTo(e2.getProbeId());
      if (probeComparison == 0) {
        return String.valueOf(e1.getIndex()).compareTo(String.valueOf(e2.getIndex()));
      }
      return probeComparison;
    });
    var iterators = indices.stream()
        .map(index -> Collections.enumeration(index.getIndex().keySet())).toList();
    var firstElements = IntStream.range(0, indices.size())
        .mapToObj(i -> new Element(
            iterators.get(i).nextElement(), i
        ))
        .toList();
    heap.addAll(firstElements);
    while (heap.size() > 0) {
      var next = heap.remove();
      var segmentIndex = indices.get(next.getIndex());
      var in = fileIOService.readBytes(
          segmentService.getPathForSegment(segmentIndex.getSegmentName()),
          segmentIndex.getIndex().get(next.getProbeId())
      );
      FileUtils.writeByteArrayToFile(mergedSegment, in, true);
      var iterator = iterators.get(next.getIndex());
      if (iterator.hasMoreElements()) {
        heap.add(new Element(iterator.nextElement(), next.getIndex()));
      }
    }
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
