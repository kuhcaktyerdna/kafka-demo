package com.demo.kafka_demo.kafka;

import com.demo.kafka_demo.model.TopicName;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class KafkaConsumer {

    @KafkaListener(topics = TopicName.DEMO, groupId = "demoGroup")
    public void consume(String message) {
        log.info("Message received: {}", message);
    }

}
