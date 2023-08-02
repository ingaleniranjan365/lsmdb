package com.spring_boot.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lsmdb.LSM;
import com.lsmdb.exception.ElementNotFoundException;
import com.lsmdb.service.LSMService;
import io.vertx.core.buffer.Buffer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.CompletableFuture;

@RestController
public class MasterController {

        private final ObjectMapper mapper = new ObjectMapper();
        private final LSMService lsmService = LSM.getLsmService();

        @GetMapping("/latest/element/{id}")
        public ResponseEntity<JsonNode> getData(final @PathVariable("id") String id)
                throws JsonProcessingException {
                try {
                        return ResponseEntity.ok(mapper.readTree(lsmService.getData(id)));
                } catch (ElementNotFoundException e) {
                        e.printStackTrace();
                        return ResponseEntity.notFound().build();
                }
        }

        @PutMapping("/element/{id}")
        public ResponseEntity<CompletableFuture<Boolean>> updatePayload(final @PathVariable("id") String id,
                                                                        final @RequestBody String payload) {
                return ResponseEntity.ok(lsmService.insert(id, Buffer.buffer(payload)));
        }
}
