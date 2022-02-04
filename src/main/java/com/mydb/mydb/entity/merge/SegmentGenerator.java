package com.mydb.mydb.entity.merge;

import com.mydb.mydb.entity.SegmentIndex;
import com.mydb.mydb.service.FileIOService;
import com.mydb.mydb.service.SegmentService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.SerializationUtils;

import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;

import static com.mydb.mydb.MydbApplication.MAX_MEM_TABLE_SIZE;
import static com.mydb.mydb.service.FileIOService.WAL_FILE;
import static java.util.concurrent.CompletableFuture.supplyAsync;

@Component
public class SegmentGenerator {

  private final FileIOService fileIOService;
  private final SegmentService segmentService;

  @Autowired
  public SegmentGenerator(FileIOService fileIOService, SegmentService segmentService) {
    this.fileIOService = fileIOService;
    this.segmentService = segmentService;
  }

  public boolean update(
      ConcurrentLinkedDeque<SegmentIndex> indices,
      Map<String, String> memTable
  ) {
    if (isMemTableFull(memTable)) {
      generate(indices, memTable);
    }
    return true;
  }

  private void generate(
      ConcurrentLinkedDeque<SegmentIndex> indices,
      Map<String, String> memTable
  ) {
    var newSegment = segmentService.getNewSegment();
    supplyAsync(() -> fileIOService.persist(newSegment, memTable))
        .thenApply(i -> updateIndices(indices, i))
        .thenApply(b -> fileIOService.persistIndices(newSegment.getBackupPath(), SerializationUtils.serialize(indices)))
        .thenApply(b -> clearMemTable(memTable))
        .thenApply(b -> WAL_FILE.delete());
  }

  private boolean clearMemTable(Map<String, String> memTable) {
    memTable.keySet().stream().toList().subList(0, MAX_MEM_TABLE_SIZE).forEach(memTable::remove);
    return true;
  }

  private boolean updateIndices(ConcurrentLinkedDeque<SegmentIndex> indices, SegmentIndex s) {
    indices.addFirst(s);
    return true;
  }

  private boolean isMemTableFull(final Map<String, String> memTable) {
    return memTable.size() >= MAX_MEM_TABLE_SIZE;
  }

}
