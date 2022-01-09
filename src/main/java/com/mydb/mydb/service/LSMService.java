package com.mydb.mydb.service;

import com.mydb.mydb.entity.Payload;
import com.mydb.mydb.exception.UnknownProbeException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

@Service
public class LSMService {

  public static final long MAX_MEMTABLE_SIZE = 2;
  private PersistenceService persistenceService;
  private SegmentService segmentService;
  private Map<String, Payload> memTable = new TreeMap<>();

  @Autowired
  public LSMService(PersistenceService persistenceService, SegmentService segmentService) {
    this.persistenceService = persistenceService;
    this.segmentService = segmentService;
  }

  public Payload insert(final Payload payload) throws IOException {
    memTable.put(payload.getProbeId(), payload);
    if(memTable.size() == MAX_MEMTABLE_SIZE) {
      persistenceService.persist(segmentService.getNewSegmentPath(), memTable);
      memTable = new TreeMap<>();
    }
    return payload;
  }

  public Payload getData(String probeId) throws UnknownProbeException {
    return Optional.ofNullable(memTable.getOrDefault(probeId, null))
        .orElseThrow(() -> new UnknownProbeException(String.format("Probe id - %s not found!", probeId)));
  }
}
