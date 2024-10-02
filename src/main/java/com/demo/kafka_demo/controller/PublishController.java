package com.demo.kafka_demo.controller;

import com.demo.kafka_demo.kafka.producer.StringKafkaProducer;
import com.demo.kafka_demo.kafka.producer.UserKafkaProducer;
import com.demo.kafka_demo.model.User;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
@RequestMapping("/api/publish")
@RequiredArgsConstructor
public class PublishController {

    private final StringKafkaProducer stringKafkaProducer;
    private final UserKafkaProducer userKafkaProducer;

    @PostMapping("/string")
    public ResponseEntity<String> publish(@RequestParam final String message) {
        log.info("Publish message: {}", message);
        stringKafkaProducer.send(message);
        return ResponseEntity.ok("Message published successfully");
    }

    @PostMapping("/object")
    public ResponseEntity<String> publish(@RequestBody final User user) {
        log.info("Publish user: {}", user);
        userKafkaProducer.send(user);
        return ResponseEntity.ok("Message published successfully");
    }

}
