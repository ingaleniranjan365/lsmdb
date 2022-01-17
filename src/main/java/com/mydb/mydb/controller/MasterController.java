package com.mydb.mydb.controller;

import com.mydb.mydb.entity.Payload;
import com.mydb.mydb.exception.UnknownProbeException;
import com.mydb.mydb.service.LSMService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.List;

@RestController
public class MasterController {

  private LSMService lsmService;

  @Autowired
  public MasterController(LSMService lsmService) {
    this.lsmService = lsmService;
  }

  @PostMapping("/echo")
  public ResponseEntity<Payload> echoPayload(final @RequestBody Payload payload) {
    return ResponseEntity.ok(payload);
  }

  @GetMapping("/merge")
  public ResponseEntity<List<Payload>> merge() throws IOException {
    return ResponseEntity.ok(lsmService.merge());
  }

  @PostMapping("/persist")
  public ResponseEntity<Payload> persistPayload(final @RequestBody Payload payload) throws IOException {
    return ResponseEntity.ok(lsmService.insert(payload));
  }

  @GetMapping("/probe/{probeId}/latest")
  public ResponseEntity<Payload> getData(final @PathVariable("probeId") String probeId) {
    try {
      return ResponseEntity.ok(lsmService.getData(probeId));
    } catch (UnknownProbeException e) {
      e.printStackTrace();
      return ResponseEntity.notFound().build();
    }
  }
}
