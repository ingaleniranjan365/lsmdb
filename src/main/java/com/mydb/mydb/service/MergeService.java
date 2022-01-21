package com.mydb.mydb.service;

import com.mydb.mydb.entity.Payload;
import com.mydb.mydb.entity.SegmentIndex;
import com.mydb.mydb.entity.SegmentMetadata;
import com.mydb.mydb.entity.merge.HeapElement;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

import static com.mydb.mydb.entity.merge.HeapElement.getHeapElementComparator;
import static com.mydb.mydb.entity.merge.HeapElement.isProbeIdPresentInList;

@Slf4j
@Component
public class MergeService {

  private final FileIOService fileIOService;
  private final SegmentService segmentService;
  private final LSMService lsmService;


  @Autowired
  public MergeService(FileIOService fileIOService, SegmentService segmentService, LSMService lsmService) {
    this.fileIOService = fileIOService;
    this.segmentService = segmentService;
    this.lsmService = lsmService;
  }

  @Scheduled(fixedRate = 10000)
  public List<Payload> merge() throws IOException {
    log.info("**************\nStarting scheduled merging!\n******************");
    var segmentIndexEnumeration = lsmService.getIndices().stream()
        .map(index -> ImmutablePair.of(Collections.enumeration(index.getIndex().keySet()), index)).toList();
    return merge(segmentIndexEnumeration);
  }

  public List<Payload> merge(final List<ImmutablePair<Enumeration<String>, SegmentIndex>> segmentIndexEnumeration)
      throws IOException {
    var mergedSegment = new File(segmentService.getNewSegmentPath());
    final Map<String, SegmentMetadata> mergedSegmentIndex = new LinkedHashMap<>();
    var heap = new PriorityQueue<>(getHeapElementComparator());

    heap.addAll(getSeedElements(segmentIndexEnumeration));

    var last = new HeapElement("", -1);
    while (!heap.isEmpty()) {
      var candidate = findNextCandidate(segmentIndexEnumeration, heap, last);
      if (couldNotFindNextElementToProcess(heap, last, candidate)) {
        break;
      }
      var segmentIndex = segmentIndexEnumeration.get(candidate.getIndex()).right;
      var in = fileIOService.readBytes(
          segmentService.getPathForSegment(segmentIndex.getSegmentName()),
          segmentIndex.getIndex().get(candidate.getProbeId())
      );
      mergedSegmentIndex.put(candidate.getProbeId(), new SegmentMetadata((int) (mergedSegment.length()), in.length));
      FileUtils.writeByteArrayToFile(mergedSegment, in, true);

      addNextHeapElementForSegment(segmentIndexEnumeration, heap, candidate);
      last = candidate;
    }
    var data = readMergedFile(mergedSegmentIndex, mergedSegment.getAbsolutePath());
    var probeIds = data.stream().map(Payload::getProbeId).toList();
    log.info(probeIds.toString());
    return data;
  }

  private List<Payload> readMergedFile(Map<String, SegmentMetadata> mergedSegmentIndex, final String path) {
    return mergedSegmentIndex.values().stream().map(v ->
            fileIOService.getPayload(path, v))
        .filter(Optional::isPresent).map(Optional::get).collect(Collectors.toList());
  }

  private HeapElement findNextCandidate(List<ImmutablePair<Enumeration<String>, SegmentIndex>> segmentIndexEnumeration,
                                        PriorityQueue<HeapElement> heap, HeapElement last) {
    var next = heap.remove();
    while (next.getProbeId().equals(last.getProbeId())) {
      addNextHeapElementForSegment(segmentIndexEnumeration, heap, next);
      if (heap.isEmpty()) {
        break;
      }
      next = heap.remove();
    }
    return next;
  }

  private boolean couldNotFindNextElementToProcess(PriorityQueue<HeapElement> heap, HeapElement last,
                                                   HeapElement next) {
    return heap.isEmpty() && next.getProbeId().equals(last.getProbeId());
  }

  private void addNextHeapElementForSegment(
      List<ImmutablePair<Enumeration<String>, SegmentIndex>> segmentIndexEnumeration,
      PriorityQueue<HeapElement> heap, HeapElement next) {
    var iterator = segmentIndexEnumeration.get(next.getIndex()).left;
    if (iterator.hasMoreElements()) {
      heap.add(new HeapElement(iterator.nextElement(), next.getIndex()));
    }
  }

  private LinkedList<HeapElement> getSeedElements(
      List<ImmutablePair<Enumeration<String>, SegmentIndex>> segmentIndexEnumeration) {
    var firstElements = new LinkedList<HeapElement>();
    int itr = 0;
    while (itr < segmentIndexEnumeration.size()) {
      var next = new HeapElement(segmentIndexEnumeration.get(itr).left.nextElement(), itr);
      while (isProbeIdPresentInList(firstElements, next.getProbeId())) {
        if (segmentIndexEnumeration.get(itr).left.hasMoreElements()) {
          next = new HeapElement(segmentIndexEnumeration.get(itr).left.nextElement(), itr);
        } else {
          break;
        }
      }
      if (!isProbeIdPresentInList(firstElements, next.getProbeId())) {
        firstElements.add(next);
      }
      itr += 1;
    }
    return firstElements;
  }

}
