package com.demo.kafka_demo.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class KafkaProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaProducer(final KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(final String message) {
        send("demo", message);
    }

    public void send(final String topic, final String message) {
        log.info("Message is: {} for topic {}", message, topic);
        kafkaTemplate.send(topic, message);
    }

}
