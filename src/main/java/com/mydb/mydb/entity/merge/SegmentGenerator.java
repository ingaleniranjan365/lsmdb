package com.mydb.mydb.entity.merge;

import com.mydb.mydb.entity.SegmentIndex;
import com.mydb.mydb.service.FileIOService;
import com.mydb.mydb.service.SegmentService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.SerializationUtils;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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
      ConcurrentLinkedDeque<SegmentIndex> indices,
      Map<String, String> memTable
  ) {
//    generateAsync(indices, memTable);
    supplyAsync(() -> generate(lock, memTable, indices));
    return true;
  }

  private boolean generate(Lock lock, Map<String, String> memTable,
                                        ConcurrentLinkedDeque<SegmentIndex> indices) {
    if(lock.tryLock()) {
      log.info("Acquired Lock!");
      try {
        if (isMemTableFull(memTable)) {
          var segment = segmentService.getNewSegment();
          var index = fileIOService.persist(segment, memTable);
          updateIndices(indices, index);
          log.info("memTable.size() before clearing: {}", memTable.size());
          clearMemTable(memTable);
          log.info("memTable.size() after clearing: {}", memTable.size());
          fileIOService.persistIndices(segment.getBackupPath(), SerializationUtils.serialize(indices));
          WAL_FILE.delete();
        }
      } catch (Exception ex) {
        ex.printStackTrace();
      }
      finally {
        lock.unlock();
        log.info("Released Lock!");
      }
    }
    return true;
  }

  private void clearMemTable(Map<String, String> memTable) {
    var probeIdsToBeRemoved = memTable.keySet().stream().toList().subList(0, MAX_MEM_TABLE_SIZE);
    probeIdsToBeRemoved.forEach(memTable::remove);
    log.info("Cleared Memtable at : {} ", LocalDateTime.now());
  }

  private void updateIndices(ConcurrentLinkedDeque<SegmentIndex> indices, SegmentIndex s) {
    indices.addFirst(s);
    log.info("Updated Indices at : {} ", LocalDateTime.now());
  }

  private boolean isMemTableFull(final Map<String, String> memTable) {
    return memTable.size() >= MAX_MEM_TABLE_SIZE;
  }

//  private void generateAsync(ConcurrentLinkedDeque<SegmentIndex> indices, Map<String, String> memTable) {
//    supplyAsync(() -> {
//      System.out.println();
//      lock.lock();
//      return isMemTableFull(memTable);
//    })
//        .thenApply(b -> b ? segmentService.getNewSegment(): null)
//        .thenApply(b -> b!=null ? fileIOService.persist(b, memTable) : null)
//        .thenApply(b -> b!=null ? updateIndices(indices, b) : null)
//        .thenApply(b -> b!=null ? clearMemTable(memTable, b) : null)
//        .thenApply(b -> {
//          lock.unlock();
//          return b;
//        })
//        .thenApply(b -> b!=null ? fileIOService.persistIndices(b.getSegment().getBackupPath(),
//            SerializationUtils.serialize(indices)) : null)
//        .thenApply(b -> b!=null ? WAL_FILE.delete() : null);
//  }
//
//  private SegmentIndex clearMemTable(Map<String, String> memTable, SegmentIndex index) {
//    var sortedStream = memTable.keySet().stream().toList().subList(0, MAX_MEM_TABLE_SIZE).stream().sorted();
//    sortedStream.forEach(memTable::remove);
//    log.info("Cleared Memtable at : {} ", LocalDateTime.now());
//    return index;
//  }
//
//    private SegmentIndex updateIndices(ConcurrentLinkedDeque<SegmentIndex> indices, SegmentIndex s) {
//      indices.addFirst(s);
//      log.info("Updated Indices at : {} ", LocalDateTime.now());
//      return s;
//    }

}
