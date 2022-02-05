package com.mydb.mydb.entity;

import com.mydb.mydb.entity.merge.SegmentGenerator;
import com.mydb.mydb.service.FileIOService;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executor;

@Component
@Getter
@Setter
@Slf4j
public class Sailee {

  private final Executor executor;
  private ConcurrentLinkedDeque<SegmentIndex> indices;
  private Map<String, String> memTable;
  private SegmentGenerator generator;
  private FileIOService fileIOService;

  public Sailee(
      @Qualifier("memTable") Map<String, String> memTable,
      @Qualifier("indices") ConcurrentLinkedDeque<SegmentIndex> indices,
      FileIOService fileIOService,
      SegmentGenerator generator,
      @Qualifier("segmentExecutor") Executor executor
  ) {
    this.indices = indices;
    this.memTable = memTable;
    this.fileIOService = fileIOService;
    this.generator = generator;
    this.executor = executor;
  }

  public CompletableFuture<Boolean> persist(final String probeId, final String payload) {
    return fileIOService.writeAheadLog(payload)
        .thenApply(b -> memTable.put(probeId, payload))
        .thenApply(b -> generator.update(indices, memTable));
  }

  public String get(final String probeId) {
    return memTable.getOrDefault(probeId, null);
  }
}
