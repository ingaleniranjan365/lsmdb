package com.lsmdb.entity.merge;

import com.lsmdb.entity.SegmentIndex;
import com.lsmdb.service.FileIOService;
import com.lsmdb.service.LSMService;
import com.lsmdb.service.SegmentService;
import io.vertx.core.buffer.Buffer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.util.Deque;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.IntStream;

import static java.util.concurrent.CompletableFuture.supplyAsync;

@Slf4j
public class SegmentGenerator {

  private final FileIOService fileIOService;
  private final SegmentService segmentService;
  private final Lock lock = new ReentrantLock();
  private final int memTableSoftLimit;
  private final int memTableHardLimit;

  public SegmentGenerator(
      FileIOService fileIOService, SegmentService segmentService,
      int memTableSoftLimit,
      int memTableHardLimit
  ) {
    this.fileIOService = fileIOService;
    this.segmentService = segmentService;
    this.memTableSoftLimit = memTableSoftLimit;
    this.memTableHardLimit = memTableHardLimit;
  }

  public boolean update(
      Deque<SegmentIndex> indices,
      Deque<String> ids,
      Map<String, Deque<Buffer>> memTable
  ) {
    return generate(indices, ids, memTable);
  }

  private boolean generate(
      Deque<SegmentIndex> indices,
      Deque<String> ids,
      Map<String, Deque<Buffer>> memTable
  ) {
    if (lock.tryLock()) {
      try {
        final var size = ids.size();
        if (isMemTableFull(size)) {
          updateHardLimitBreach(size);
          flushMultipleSegments(indices, ids, memTable, size);
        }
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      } finally {
        lock.unlock();
      }
    }
    return true;
  }


  private void flushMultipleSegments(
      final Deque<SegmentIndex> indices,
      final Deque<String> ids,
      final Map<String, Deque<Buffer>> memTable,
      final int size
  ) {
    LinkedList<ImmutablePair<Integer, Integer>> ranges = getRanges(size);

    ranges.parallelStream()
        .map(range -> ImmutablePair.of(range, segmentService.getNewSegment()))
        .map(pair -> supplyAsync(
            () -> ImmutablePair.of(fileIOService.persist(pair.right, ids, memTable, pair.left), pair.right))
        )
        .map(CompletableFuture::join)
        .forEach(pair -> updateIndices(indices, pair.left));

    clearIds(ids, ImmutablePair.of(0, ranges.getLast().right));
    updateHardLimitBreach(ids.size());

    FileIOService.STAGED_WAL_FILE.delete();
    FileIOService.WAL_FILE.renameTo(FileIOService.STAGED_WAL_FILE);
  }

  private void clearIds(
      Deque<String> ids,
      ImmutablePair<Integer, Integer> range
  ) {
    IntStream.range(range.left, range.right).forEach(i -> {
        try {
          ids.removeFirst();
        } catch (Exception ex) {
          ex.printStackTrace();
        }
      }
    );
  }

  private void updateIndices(Deque<SegmentIndex> indices, SegmentIndex s) {
    indices.addFirst(s);
  }

  private boolean isMemTableFull(final int payloadCount) {
    return payloadCount >= memTableSoftLimit;
  }

  public void updateHardLimitBreach(final int payloadCount) {
    LSMService.hardLimitBreached = payloadCount >= memTableHardLimit;
  }

  private LinkedList<ImmutablePair<Integer, Integer>> getRanges(int size) {
    var ranges = new LinkedList<ImmutablePair<Integer, Integer>>();
    var start = 0;
    var end = memTableSoftLimit;
    while (size >= end) {
      ranges.add(ImmutablePair.of(start, end));
      start = end + 1;
      end += memTableSoftLimit;
    }
    return ranges;
  }

}
