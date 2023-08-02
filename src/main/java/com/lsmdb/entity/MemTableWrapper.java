package com.lsmdb.entity;

import com.lsmdb.entity.merge.SegmentGenerator;
import com.lsmdb.service.FileIOService;
import io.vertx.core.buffer.Buffer;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.util.Deque;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;

@Getter
@Setter
@Slf4j
public class MemTableWrapper {

  private Deque<SegmentIndex> indices;
  private SegmentGenerator generator;
  private FileIOService fileIOService;
  private Deque<String> ids;
  private Map<String, Deque<Buffer>> memTable;

  public MemTableWrapper(
      ImmutablePair<Deque<String>, Map<String, Deque<Buffer>>> memTableData,
      Deque<SegmentIndex> indices,
      FileIOService fileIOService,
      SegmentGenerator generator
  ) {
    this.indices = indices;
    this.fileIOService = fileIOService;
    this.generator = generator;
    this.ids = memTableData.getLeft();
    this.memTable = memTableData.getRight();
  }

  public CompletableFuture<Boolean> persist(final String id, final Buffer payload) {
    return fileIOService.writeAheadLog(payload)
        .thenApply(b -> put(id, payload))
        .thenApply(b -> generator.update(indices, ids, memTable));
  }

  private boolean put(final String id, final Buffer payload) {
    ids.addLast(id);
    if (memTable.containsKey(id)) {
      memTable.get(id).addLast(payload);
    } else {
      var list = new ConcurrentLinkedDeque<Buffer>();
      list.addLast(payload);
      memTable.put(id, list);
    }
    return true;
  }

  public Buffer get(final String id) {
    var list = memTable.getOrDefault(id, null);
    if (list != null && !list.isEmpty()) {
      return list.getLast();
    }
    return null;
  }
}
