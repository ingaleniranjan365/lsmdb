package com.mydb.mydb.entity;

import com.mydb.mydb.entity.merge.SegmentGenerator;
import com.mydb.mydb.service.FileIOService;
import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;

@Component
@Getter
@Setter
public class Sailee {

  private ConcurrentLinkedDeque<SegmentIndex> indices;
  private Map<String, String> memTable;
  private SegmentGenerator generator;
  private FileIOService fileIOService;

  public Sailee(
      @Qualifier("memTable") Map<String, String> memTable,
      @Qualifier("indices") ConcurrentLinkedDeque<SegmentIndex> indices,
      FileIOService fileIOService,
      SegmentGenerator generator
  ) {
    this.indices = indices;
    this.memTable = memTable;
    this.fileIOService = fileIOService;
    this.generator = generator;
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
