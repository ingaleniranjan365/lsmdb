package com.mydb.mydb.entity;

import com.mydb.mydb.entity.merge.SegmentGenerator;
import com.mydb.mydb.service.FileIOService;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Deque;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executor;

import static java.util.concurrent.CompletableFuture.supplyAsync;

@Component
@Getter
@Setter
@Slf4j
public class MemTableWrapper {

  private Deque<SegmentIndex> indices;
  private SegmentGenerator generator;
  private FileIOService fileIOService;
  private Deque<String> probeIds;
  private Map<String, Deque<String>> memTable;
  private Executor executor;
  private boolean useFixedThreadPool;
  private boolean disableWal;

  public MemTableWrapper(
      @Qualifier("memTableData") ImmutablePair<Deque<String>, Map<String, Deque<String>>> memTableData,
      @Qualifier("indices") Deque<SegmentIndex> indices,
      FileIOService fileIOService,
      SegmentGenerator generator,
      @Qualifier("fixedThreadPool") Executor executor,
      @Value("${config.useFixedThreadPool}") boolean useFixedThreadPool,
      @Value("${config.disableWal}") boolean disableWal
  ) {
    this.indices = indices;
    this.fileIOService = fileIOService;
    this.generator = generator;
    this.probeIds = memTableData.getLeft();
    this.memTable = memTableData.getRight();
    this.executor = executor;
    this.useFixedThreadPool = useFixedThreadPool;
    this.disableWal = disableWal;
  }

  public CompletableFuture<Boolean> persist(final String probeId, final String payload) {
    if(disableWal) {
      if(useFixedThreadPool) {
        return supplyAsync(() -> put(probeId, payload), executor)
            .thenApply(b -> generator.update(indices, probeIds, memTable));
      }
      return supplyAsync(() -> put(probeId, payload))
          .thenApply(b -> generator.update(indices, probeIds, memTable));
    }
    if(useFixedThreadPool) {
      return fileIOService.writeAheadLog(payload, executor)
          .thenApply(b -> put(probeId, payload))
          .thenApply(b -> generator.update(indices, probeIds, memTable));
    }
    return fileIOService.writeAheadLog(payload)
        .thenApply(b -> put(probeId, payload))
        .thenApply(b -> generator.update(indices, probeIds, memTable));
  }

  private boolean put(final String probeId, final String payload) {
    probeIds.addLast(probeId);
    if(memTable.containsKey(probeId)) {
      memTable.get(probeId).addLast(payload);
    } else {
      var list = new ConcurrentLinkedDeque<String>();
      list.addLast(payload);
      memTable.put(probeId, list);
    }
    return true;
  }

  public String get(final String probeId) {
    var list = memTable.getOrDefault(probeId, null);
    if(list!=null && !list.isEmpty()) {
      return list.getLast();
    }
    return null;
  }
}
