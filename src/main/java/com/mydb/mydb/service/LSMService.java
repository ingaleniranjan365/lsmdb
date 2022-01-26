package com.mydb.mydb.service;

import com.mydb.mydb.Config;
import com.mydb.mydb.entity.Index;
import com.mydb.mydb.entity.Payload;
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

  public static final long MAX_MEM_TABLE_SIZE = 8;
  public static final int MAX_PAYLOAD_SIZE = 20000;
  private final FileIOService fileIOService;
  private final SegmentService segmentService;
  private final MergeService mergeService;
  private Index index;
  private Map<String, Payload> memTable;

  @Autowired
  public LSMService(@Qualifier("memTable") Map<String, Payload> memTable,
      @Qualifier("index") Index index, FileIOService fileIOService,
                    SegmentService segmentService, MergeService mergeService
  ) {
    this.fileIOService = fileIOService;
    this.segmentService = segmentService;
    this.mergeService = mergeService;
    this.index = index;
    this.memTable = memTable;
  }

  /**
   * TODO: 1. Make sure indices.add is executed thread safe
   *       2. Make sure indices is thread safe object.
   */
  @Scheduled(fixedRate = 10000)
  public void merge() throws IOException {
    log.info("**************\nStarting scheduled merging!\n******************");
    var mergeSegment = segmentService.getNewSegment();
    int size = index.getIndices().size();
    var segmentEnumeration = getSegmentIndexEnumeration();
    var mergedSegmentIndex = mergeService.merge(segmentEnumeration, mergeSegment.getSegmentPath());
    if(!mergedSegmentIndex.isEmpty()) {
      var oldIndex = index;
      IntStream.range(0, size).forEach(x -> index.getIndices().removeLast());
      index.getIndices().addLast(new SegmentIndex(mergeSegment.getSegmentName(), mergedSegmentIndex));
      persistIndex(index);
      deleteSegmentsAfterMerging(segmentEnumeration, oldIndex);
    }
  }

  private void deleteSegmentsAfterMerging(
      final List<ImmutablePair<Enumeration<String>, SegmentIndex>> segmentIndexEnumeration,
      Index oldIndex
  ) {
    segmentIndexEnumeration.parallelStream().map(x -> x.getRight().getSegmentName())
        .map(segmentService::getPathForSegment).forEach(z -> new File(z).delete());
    deleteOldIndex(oldIndex);
  }

  private void deleteOldIndex(Index oldIndex) {
    if(oldIndex.getBackUpName()!=null) {
      new File(segmentService.getPathForBackup(oldIndex.getBackUpName())).delete();
    }
  }

  public List<ImmutablePair<Enumeration<String>, SegmentIndex>> getSegmentIndexEnumeration() {
    return index.getIndices().stream()
        .filter(i -> new File(segmentService.getPathForSegment(i.getSegmentName())).exists())
        .map(index -> ImmutablePair.of(Collections.enumeration(index.getSegmentIndex().keySet()), index)).toList();
  }

  public Payload insert(final Payload payload) throws IOException, PayloadTooLargeException {
    writeAppendLog(payload);
    memTable.put(payload.getProbeId(), payload);
    if (memTable.size() >= MAX_MEM_TABLE_SIZE) {
      var newIndex = index.toBuilder().build();
      newIndex.getIndices().addFirst(fileIOService.persist(segmentService.getNewSegment(), memTable));
      persistIndex(newIndex);
      var oldIndex = index;
      index = newIndex;
      deleteOldIndex(oldIndex);
      resetMemtableAndWAL();
    }
    return payload;
  }

  private void resetMemtableAndWAL() {
    memTable = new TreeMap<>();
    clearWriteAppendLog();
  }

  private void clearWriteAppendLog() {
    File file =  new File(Config.DEFAULT_WAL_FILE_PATH);
    if(file.exists()) {
      file.delete();
    }
  }

  private void writeAppendLog(Payload payload) throws PayloadTooLargeException {
    File file =  new File(Config.DEFAULT_WAL_FILE_PATH);
    var fixedBytes = new byte[MAX_PAYLOAD_SIZE];
    var bytes =  SerializationUtils.serialize(payload);
    if(bytes!=null && bytes.length > 0) {
      if(bytes.length > MAX_PAYLOAD_SIZE) {
        throw new PayloadTooLargeException();
      }
      System.arraycopy(bytes, 0, fixedBytes, 0, bytes.length);
      try {
        FileUtils.writeByteArrayToFile(file, fixedBytes, true);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  private void persistIndex(Index index) {
    if (!index.getIndices().isEmpty()) {
      try {
        var bytes = SerializationUtils.serialize(index);
        var backup = segmentService.getNewBackup();
        var indexFile = new File(backup.getBackupPath());
        FileUtils.writeByteArrayToFile(indexFile, bytes);
        index.setBackUpName(backup.getBackupName());
      } catch (IOException ex) {
        throw new RuntimeException(ex.getMessage());
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

    var segmentIndex =
        index.getIndices().stream().filter(x -> x.getSegmentIndex().containsKey(probeId)).findFirst().orElse(null);
    return Optional.ofNullable(segmentIndex)
        .map(i -> segmentService.getPathForSegment(i.getSegmentName()))
        .map(p -> fileIOService.getPayload(p, segmentIndex.getSegmentIndex().get(probeId)))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .orElse(null);
  }
}
