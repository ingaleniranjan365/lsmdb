package com.mydb.mydb.entity.merge;

import com.mydb.mydb.entity.SegmentIndex;
import com.mydb.mydb.service.FileIOService;
import com.mydb.mydb.service.LSMService;
import com.mydb.mydb.service.SegmentService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.SerializationUtils;

import java.util.Deque;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.IntStream;

import static com.mydb.mydb.service.FileIOService.WAL_FILE;
import static java.util.concurrent.CompletableFuture.supplyAsync;

@Component
@Slf4j
public class SegmentGenerator {

  private final FileIOService fileIOService;
  private final SegmentService segmentService;
  private final Lock lock = new ReentrantLock();
  private final boolean flushMultipleRanges;
  private final Executor executor;
  private final boolean useFixedThreadPool;
  private final int memTableSoftLimit;
  private final int memTableHardLimit;

  @Autowired
  public SegmentGenerator(FileIOService fileIOService, SegmentService segmentService,
                          @Value("${config.flushMultipleRanges}") boolean flushMultipleRanges,
                          @Qualifier("fixedThreadPool") Executor executor,
                          @Value("${config.useFixedThreadPool}") boolean useFixedThreadPool,
                          @Value("${config.memTableSoftLimit}") int memTableSoftLimit,
                          @Value("${config.memTableHardLimit}") int memTableHardLimit
  ) {
    this.fileIOService = fileIOService;
    this.segmentService = segmentService;
    this.flushMultipleRanges = flushMultipleRanges;
    this.executor = executor;
    this.useFixedThreadPool = useFixedThreadPool;
    this.memTableSoftLimit = memTableSoftLimit;
    this.memTableHardLimit = memTableHardLimit;
  }

  public boolean update(
      Deque<SegmentIndex> indices,
      Deque<String> probeIds,
      Map<String, Deque<String>> memTable
  ) {
    if(useFixedThreadPool) {
      supplyAsync(() -> generate(indices, probeIds, memTable), executor);
    } else {
      supplyAsync(() -> generate(indices, probeIds, memTable));
    }
    return true;
  }

  private boolean generate(
      Deque<SegmentIndex> indices,
      Deque<String> probeIds,
      Map<String, Deque<String>> memTable
  ) {
    if (lock.tryLock()) {
      try {
        if (isMemTableFull(memTable)) {
          if(flushMultipleRanges) {
            persist(indices, probeIds, memTable);
          } else {
            var segment = segmentService.getNewSegment();
            var size = memTable.size();
            var index = fileIOService.persist(segment, probeIds, memTable, ImmutablePair.of(0, size));
            updateIndices(indices, index);
            clearMemTable(probeIds, memTable, ImmutablePair.of(0, size));
            if(useFixedThreadPool) {
              supplyAsync(
                  () -> fileIOService.persistIndices(segment.getBackupPath(), SerializationUtils.serialize(indices)) &&
                      WAL_FILE.delete(), executor);
            } else {
              supplyAsync(
                  () -> fileIOService.persistIndices(segment.getBackupPath(), SerializationUtils.serialize(indices)) &&
                      WAL_FILE.delete());
            }
          }
        }
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      } finally {
        lock.unlock();
      }
    }
    return true;
  }

  private void persist(
      final Deque<SegmentIndex> indices,
      final Deque<String> probeIds,
      final Map<String, Deque<String>> memTable
  ) {
    LinkedList<ImmutablePair<Integer, Integer>> ranges = getRanges(memTable);

    ranges.stream()
        .map(range -> ImmutablePair.of(range, segmentService.getNewSegment()))
        .map(pair -> {
          if(useFixedThreadPool) {
            return supplyAsync(() -> ImmutablePair.of(fileIOService.persist(pair.right, probeIds, memTable, pair.left), pair.right), executor);
          }
          return supplyAsync(() -> ImmutablePair.of(fileIOService.persist(pair.right, probeIds, memTable, pair.left), pair.right));
        })
        .map(CompletableFuture::join)
        .forEach(pair -> {
          updateIndices(indices, pair.left);
          if(useFixedThreadPool) {
            supplyAsync(() -> fileIOService.persistIndices(pair.right.getBackupPath(),
                SerializationUtils.serialize(indices)), executor);
          } else {
            supplyAsync(() -> fileIOService.persistIndices(pair.right.getBackupPath(),
                SerializationUtils.serialize(indices)));
          }
        });

    clearMemTable(probeIds, memTable, ImmutablePair.of(0, ranges.getLast().right));
    WAL_FILE.delete();
  }

  private LinkedList<ImmutablePair<Integer, Integer>> getRanges(Map<String, Deque<String>> memTable) {
    var ranges = new LinkedList<ImmutablePair<Integer, Integer>>();
    var size = memTable.size();
    var start = 0;
    var end = memTableSoftLimit;
    while(size >= end) {
      ranges.add(ImmutablePair.of(start, end));
      start = end + 1;
      end += memTableSoftLimit;
    }
    return ranges;
  }

  private void clearMemTable(
      Deque<String> probeIds,
      Map<String, Deque<String>> memTable,
      ImmutablePair<Integer, Integer> range
  ) {
    IntStream.range(range.left, range.right).forEach(i -> {
      var probeId = probeIds.removeFirst();
      var list = memTable.get(probeId);
      list.removeFirst();
    });
  }

  private void updateIndices(Deque<SegmentIndex> indices, SegmentIndex s) {
    indices.addFirst(s);
  }

  private boolean isMemTableFull(final Map<String, Deque<String>> memTable) {
    int payloadCount = memTable.keySet().stream().map(k -> memTable.get(k).size()).reduce(0, Integer::sum);
    LSMService.hardLimitReached = payloadCount >= memTableHardLimit;
    return payloadCount >= memTableSoftLimit;
  }

}
