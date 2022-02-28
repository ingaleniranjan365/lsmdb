package com.mydb.mydb.entity.merge;

import com.mydb.mydb.entity.SegmentIndex;
import com.mydb.mydb.service.FileIOService;
import com.mydb.mydb.service.SegmentService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.SerializationUtils;

import java.util.Deque;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.IntStream;

import static com.mydb.mydb.MydbApplication.MAX_MEM_TABLE_SIZE;
import static com.mydb.mydb.service.FileIOService.WAL_FILE;
import static java.util.concurrent.CompletableFuture.supplyAsync;

@Component
@Slf4j
public class SegmentGenerator {

  private final FileIOService fileIOService;
  private final SegmentService segmentService;
  private final Lock lock = new ReentrantLock();

  @Autowired
  public SegmentGenerator(FileIOService fileIOService, SegmentService segmentService) {
    this.fileIOService = fileIOService;
    this.segmentService = segmentService;
  }

  public boolean update(
      Deque<SegmentIndex> indices,
      Deque<String> probeIds,
      Map<String, String> memTable
  ) {
    supplyAsync(() -> generate(indices, probeIds, memTable));
    return true;
  }

  private boolean generate(
      Deque<SegmentIndex> indices,
      Deque<String> probeIds,
      Map<String, String> memTable
  ) {
    if (lock.tryLock()) {
      try {
        if (isMemTableFull(memTable)) {
          var segment = segmentService.getNewSegment();
          var size = memTable.size();
          var index = fileIOService.persist(segment, probeIds, memTable, size);
          updateIndices(indices, index);
          clearMemTable(probeIds, memTable, size);
          supplyAsync(
              () -> fileIOService.persistIndices(segment.getBackupPath(), SerializationUtils.serialize(indices)) &&
                  WAL_FILE.delete());
        }
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      } finally {
        lock.unlock();
      }
    }
    return true;
  }

  private void clearMemTable(
      Deque<String> probeIds,
      Map<String, String> memTable,
      int size
  ) {
    IntStream.range(0, size).forEach(i -> {
      var probeId = probeIds.remove();
      memTable.remove(probeId);
    });
  }

  private void updateIndices(Deque<SegmentIndex> indices, SegmentIndex s) {
    indices.addFirst(s);
  }

  private boolean isMemTableFull(final Map<String, String> memTable) {
    return memTable.size() >= MAX_MEM_TABLE_SIZE;
  }

}
