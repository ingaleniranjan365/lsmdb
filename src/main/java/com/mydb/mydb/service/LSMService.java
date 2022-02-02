package com.mydb.mydb.service;

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
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.stream.IntStream;

import static com.mydb.mydb.Config.DEFAULT_WAL_FILE_PATH;
import static com.mydb.mydb.MydbApplication.MAX_MEM_TABLE_SIZE;

@Slf4j
@Service
public class LSMService {


  public static final int MAX_PAYLOAD_SIZE = 20000;
  private final FileIOService fileIOService;
  private final SegmentService segmentService;
  private final MergeService mergeService;
  private final ConcurrentLinkedDeque<SegmentIndex> indices;

  private Map<String, String> memTableForRead;
  private Map<String, String> memTableForReadAndWrite;
  private static final File WAL_FILE = new File(DEFAULT_WAL_FILE_PATH);

  @Autowired
  public LSMService(@Qualifier("memTable") Map<String, String> memTable,
                    @Qualifier("indices") ConcurrentLinkedDeque<SegmentIndex> indices, FileIOService fileIOService,
                    SegmentService segmentService, MergeService mergeService
  ) {
    this.fileIOService = fileIOService;
    this.segmentService = segmentService;
    this.mergeService = mergeService;
    this.indices = indices;
    this.memTableForRead = memTable;
    this.memTableForReadAndWrite = memTable;
    log.info("Constant imported with value {}", MAX_MEM_TABLE_SIZE);
  }

  @Scheduled(initialDelay = 10000, fixedDelay = 15000)
  public void merge() throws IOException {
    log.info("**************\nStarting scheduled merging!\n******************");
    var segmentEnumeration = getSegmentIndexEnumeration();
    var segmentIndexCountToBeRemoved = segmentEnumeration.size();
    var mergeSegment = segmentService.getNewSegment();
    segmentEnumeration = segmentEnumeration.stream()
        .filter(i -> new File(segmentService.getPathForSegment(i.getRight().getSegment().getSegmentName())).exists()).toList();
    if(segmentEnumeration.size() > 1) {
      var mergedSegmentIndex = mergeService.merge(segmentEnumeration, mergeSegment.getSegmentPath());
      IntStream.range(0, segmentIndexCountToBeRemoved).forEach(x -> indices.removeLast());
      indices.addLast(new SegmentIndex(mergeSegment, mergedSegmentIndex));
      fileIOService.persistIndices(mergeSegment.getBackupPath(), SerializationUtils.serialize(indices));
      deleteMergedSegments(segmentEnumeration);
    }
  }

  private void deleteMergedSegments(
      final List<ImmutablePair<Enumeration<String>, SegmentIndex>> segmentIndexEnumeration) {
    segmentIndexEnumeration.parallelStream().map(x -> x.getRight().getSegment())
     .forEach(segment -> {
       try {
         new File(segment.getSegmentPath()).delete();
         new File(segment.getBackupPath()).delete();
       } catch (RuntimeException exception) {
         exception.printStackTrace();
       }
     });
  }


  public List<ImmutablePair<Enumeration<String>, SegmentIndex>> getSegmentIndexEnumeration() {
    return indices.stream()
        .map(j -> ImmutablePair.of(Collections.enumeration(j.getSegmentIndex().keySet()), j)).toList();
  }


  public String insert(final String probeId, final String payload) throws PayloadTooLargeException {
    try {
      writeAheadLog(payload);
    } catch (PayloadTooLargeException ex) {
      throw ex;
    } catch (RuntimeException ex) {
      ex.printStackTrace();
    }
    memTableForReadAndWrite.put(probeId, payload);
    boolean flushMemTable = false;
    synchronized (memTableForReadAndWrite) {
      if(memTableForReadAndWrite.size() >= MAX_MEM_TABLE_SIZE) {
        flushMemTable = true;
        memTableForReadAndWrite = new TreeMap<>();
      }
    }

    /**
     *   Here we are choosing go with a non synchronised implementation under the assumption that
     *   once memTableForReadAndWrite will be set to a new tree map in above synchronised block,
     *   the thread that is going to push the memtable to disk will have enough time to do so
     *   before memtable gets full again and hence following if block will not get executed
     *   concurrently by 2 threads most likely.
     *   If however, 2 (or more) threads end up executing following if block in parallel, a more recent
     *   version of indices might end up in a backup file with lower count value which might result in
     *   some data loss in case of crash recovery but for now, we accept that tradeoff
     */

    if (flushMemTable) {
//      synchronized (indices) {
          try {
            var newSegment = segmentService.getNewSegment();
            var newSegmentIndex = fileIOService.persist(
                newSegment, memTableForRead
            );
            indices.addFirst(newSegmentIndex);
            fileIOService.persistIndices(newSegment.getBackupPath(), SerializationUtils.serialize(indices));
          } catch (RuntimeException ex) {
            ex.printStackTrace();
          }
//      }
      memTableForRead = memTableForReadAndWrite;

      /**
       * After following execution we might end up deleting a piece of data that has not yet been persisted to
       * disk since, while this segment is being flushed to disk, we night write more data to WAL.
       * For now, we accept this trade off for simplicity of implementation
       */
      synchronized (WAL_FILE) {
        try {
          WAL_FILE.delete(); // reset WAL
        } catch (RuntimeException ex) {
          ex.printStackTrace();
        }
      }
    }
    return payload;
  }

  private void writeAppendLog(String payload) throws PayloadTooLargeException {
    var fixedBytes = new byte[MAX_PAYLOAD_SIZE];
    var bytes = SerializationUtils.serialize(payload);
    if (bytes != null && bytes.length > 0) {
      if (bytes.length > MAX_PAYLOAD_SIZE) {
        throw new PayloadTooLargeException();
      }
      System.arraycopy(bytes, 0, fixedBytes, 0, bytes.length);
      try {
        synchronized (WAL_FILE) {
          FileUtils.writeByteArrayToFile(WAL_FILE, fixedBytes, true);
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  private void writeAheadLog(String payload) throws PayloadTooLargeException {
    var bytes = (payload + "Sailee").getBytes(StandardCharsets.UTF_8);
    if (bytes.length > 0) {
      try {
        synchronized (WAL_FILE) {
          FileUtils.writeByteArrayToFile(WAL_FILE, bytes, true);
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  public String getData(String probeId) throws UnknownProbeException {
    var data = Optional.ofNullable(memTableForRead.getOrDefault(probeId, null))
        .orElse(memTableForReadAndWrite.getOrDefault(probeId, null));
    if (data == null) {
      var dataFromSegments = getDataFromSegments(probeId);
      if (dataFromSegments == null) {
        throw new UnknownProbeException(String.format("Probe id - %s not found!", probeId));
      }
      return dataFromSegments;
    }
    return data;
  }

  private String getDataFromSegments(final String probeId) {

    var segmentIndex = indices.stream().filter(x -> x.getSegmentIndex().containsKey(probeId)).findFirst().orElse(null);
    return Optional.ofNullable(segmentIndex)
        .map(i -> segmentService.getPathForSegment(i.getSegment().getSegmentName()))
        .map(p -> fileIOService.getPayload(p, segmentIndex.getSegmentIndex().get(probeId)))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .orElse(null);
  }
}
