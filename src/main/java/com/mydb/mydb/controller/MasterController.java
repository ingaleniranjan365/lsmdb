package com.mydb.mydb.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mydb.mydb.exception.UnknownProbeException;
import com.mydb.mydb.service.LSMService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.CompletableFuture;

@RestController
public class MasterController {

  private final ObjectMapper mapper;
  private final LSMService lsmService;

  @Autowired
  public MasterController(
      LSMService lsmService,
      @Qualifier("mapper") ObjectMapper mapper
  ) {
    this.lsmService = lsmService;
    this.mapper = mapper;
  }

  @PostMapping("/echo")
  public ResponseEntity<String> echoPayload(final @RequestBody String payload) {
    return ResponseEntity.ok(payload);
  }

  @GetMapping("/probe/{probeId}/latest")
  public ResponseEntity<JsonNode> getData(final @PathVariable("probeId") String probeId)
      throws JsonProcessingException {
    try {
      return ResponseEntity.ok(mapper.readTree(lsmService.getData(probeId)));
    } catch (UnknownProbeException e) {
      e.printStackTrace();
      return ResponseEntity.notFound().build();
    }
  }

  @PutMapping("/probe/{probeId}/event/{eventId}")
  public ResponseEntity<CompletableFuture<Boolean>> updatePayload(final @PathVariable("probeId") String probeId,
                                                                  final @PathVariable("eventId") String eventId,
                                                                  final @RequestBody String payload)
  {
    return ResponseEntity.ok(lsmService.insert(probeId, payload));
  }
}
