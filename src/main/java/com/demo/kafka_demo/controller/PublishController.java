package com.demo.kafka_demo.controller;

import com.demo.kafka_demo.kafka.KafkaProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
@RequestMapping("/api/publish")
public class PublishController {

    private final KafkaProducer kafkaProducer;

    public PublishController(final KafkaProducer kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    @PostMapping
    public ResponseEntity<String> publish(@RequestParam final String message) {
        log.info("Publish message: {}", message);
        kafkaProducer.send(message);
        return ResponseEntity.ok("Message published successfully");
    }

}
