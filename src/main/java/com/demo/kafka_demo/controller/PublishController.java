package com.demo.kafka_demo.controller;

import com.demo.kafka_demo.kafka.producer.StringKafkaProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/api/publish")
public class PublishController {

    private final StringKafkaProducer stringKafkaProducer;

    public PublishController(final StringKafkaProducer stringKafkaProducer) {
        this.stringKafkaProducer = stringKafkaProducer;
    }

    @PostMapping
    public ResponseEntity<String> publish(@RequestParam final String message) {
        log.info("Publish message: {}", message);
        stringKafkaProducer.send(message);
        return ResponseEntity.ok("Message published successfully");
    }

}
