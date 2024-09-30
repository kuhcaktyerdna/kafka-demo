package com.demo.kafka_demo.kafka;

import com.demo.kafka_demo.model.TopicName;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class KafkaProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaProducer(final KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(final String message) {
        send(TopicName.DEMO, message);
    }

    public void send(final String topic, final String message) {
        log.info("Message produced: {} for topic {}", message, topic);
        kafkaTemplate.send(topic, message);
    }

}
