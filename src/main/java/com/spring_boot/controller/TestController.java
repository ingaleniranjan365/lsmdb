package com.spring_boot.controller;


import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TestController {
        @PostMapping("/echo")
        public ResponseEntity<String> echoPayload(final @RequestBody String payload) {
                return ResponseEntity.ok(payload);
        }
}
