package com.mydb.mydb.service;

import com.mydb.mydb.Config;
import com.mydb.mydb.entity.Payload;
import com.mydb.mydb.entity.Segment;
import com.mydb.mydb.entity.SegmentIndex;
import com.mydb.mydb.exception.PayloadTooLargeException;
import com.mydb.mydb.exception.UnknownProbeException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.util.SerializationUtils;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.stream.IntStream;

@Slf4j
@Service
public class LSMService {

  public static final long MAX_MEM_TABLE_SIZE = 10000;
  public static final int MAX_PAYLOAD_SIZE = 20000;
  private final FileIOService fileIOService;
  private final SegmentService segmentService;
  private final MergeService mergeService;
  private final ConcurrentLinkedDeque<SegmentIndex> indices;
  private Map<String, Payload> memTable;

  private Map<String, Payload> memTableForRead = memTable;
  private Map<String, Payload> memTableForWrite = memTable;

  @Autowired
  public LSMService(@Qualifier("memTable") Map<String, Payload> memTable,
                    @Qualifier("indices") ConcurrentLinkedDeque<SegmentIndex> indices, FileIOService fileIOService,
                    SegmentService segmentService, MergeService mergeService
  ) {
    this.fileIOService = fileIOService;
    this.segmentService = segmentService;
    this.mergeService = mergeService;
    this.indices = indices;
    this.memTable = memTable;
  }

  // count: 20
  @Scheduled(initialDelay = 20000, fixedDelay = 30000)
  public void merge() throws IOException {
    log.info("**************\nStarting scheduled merging!\n******************");
    var mergeSegment = segmentService.getNewSegment(); //10
    var oldBackupPath = segmentService.getCurrentBackupPath();
    var segmentEnumeration = getSegmentIndexEnumeration();
    var segmentIndexCountToBeRemoved = segmentEnumeration.size();
    segmentEnumeration = segmentEnumeration.stream()
        .filter(i -> new File(segmentService.getPathForSegment(i.getRight().getSegmentName())).exists()).toList();
    var mergedSegmentIndex = mergeService.merge(segmentEnumeration, mergeSegment.getSegmentPath());
    // Issue here, might put config in an inconsistent state
    // TODO: Verify this is the only place in entire app where we remove from indices
    IntStream.range(0, segmentIndexCountToBeRemoved).forEach(x -> indices.removeLast());
    indices.addLast(new SegmentIndex(mergeSegment.getSegmentName(), mergedSegmentIndex));
    // TODO: think over parallel execution of this
    persistIndices();
    deleteMergedSegmentsAndOldIndexBackup(segmentEnumeration, oldBackupPath);
  }

  private void deleteMergedSegmentsAndOldIndexBackup(
      final List<ImmutablePair<Enumeration<String>, SegmentIndex>> segmentIndexEnumeration,
      final String oldBackupPath
  ) {
    segmentIndexEnumeration.parallelStream().map(x -> x.getRight().getSegmentName())
        .map(segmentService::getPathForSegment).forEach(z -> new File(z).delete());
    deleteOldIndexBackup(oldBackupPath);
  }

  // TODO: This function need not be synchronized
  private synchronized void deleteOldIndexBackup(final String oldBackupPath) {
    try {
      new File(oldBackupPath).delete();
    } catch (RuntimeException ex) {
      ex.printStackTrace();
    }
  }

  public List<ImmutablePair<Enumeration<String>, SegmentIndex>> getSegmentIndexEnumeration() {
    return indices.stream()
        .map(j -> ImmutablePair.of(Collections.enumeration(j.getSegmentIndex().keySet()), j)).toList();
  }

  // TODO: this entire function need not be synchronous
  public synchronized Payload insert(final Payload payload) throws IOException, PayloadTooLargeException {
    writeAppendLog(payload);
    memTable.put(payload.getProbeId(), payload);
    if (memTable.size() >= MAX_MEM_TABLE_SIZE) {
      Segment newSegment;
      byte[] bytes;
      newSegment = segmentService.getNewSegment();
      var newSegmentIndex = fileIOService.persist(
          newSegment, memTable
      );
      indices.addFirst(newSegmentIndex);
      bytes = SerializationUtils.serialize(indices);
      persistIndices(newSegment.getBackupPath(), bytes);
      resetMemTableAndWAL();
    }
    return payload;
  }

  private synchronized void resetMemTableAndWAL() {
    // TODO: Update this implementation by maintaining 2 references of memtable
    memTable = new TreeMap<>();
    /***
     *  Here we might end up loosing some data if another thread has written more records to WAL by the time
     *  control reaches here and we have accepted that tradeoff for now
     */
    clearWriteAppendLog();
  }

  private void clearWriteAppendLog() {
    File file = new File(Config.DEFAULT_WAL_FILE_PATH);
    file.delete();
  }

  private void writeAppendLog(Payload payload) throws PayloadTooLargeException {
    File file = new File(Config.DEFAULT_WAL_FILE_PATH);
    var fixedBytes = new byte[MAX_PAYLOAD_SIZE];
    var bytes = SerializationUtils.serialize(payload);
    if (bytes != null && bytes.length > 0) {
      if (bytes.length > MAX_PAYLOAD_SIZE) {
        throw new PayloadTooLargeException();
      }
      // TODO: following line should be executed synchronously by taking a write lock on WAL file
      System.arraycopy(bytes, 0, fixedBytes, 0, bytes.length);
      try {
        FileUtils.writeByteArrayToFile(file, fixedBytes, true);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  // TODO: Everything in this function need not be synchronised
  public synchronized void persistIndices(final String newBackupPath, final byte[] indicesBytes) {
    try {
      var newIndexFile = new File(newBackupPath);
      FileUtils.writeByteArrayToFile(newIndexFile, indicesBytes);
    } catch (IOException | RuntimeException ex) {
      ex.printStackTrace();
    }
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

    var segmentIndex = indices.stream().filter(x -> x.getSegmentIndex().containsKey(probeId)).findFirst().orElse(null);
    return Optional.ofNullable(segmentIndex)
        .map(i -> segmentService.getPathForSegment(i.getSegmentName()))
        .map(p -> fileIOService.getPayload(p, segmentIndex.getSegmentIndex().get(probeId)))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .orElse(null);
  }
}
