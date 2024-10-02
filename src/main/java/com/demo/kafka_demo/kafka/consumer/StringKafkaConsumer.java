package com.demo.kafka_demo.kafka.consumer;

import com.demo.kafka_demo.kafka.KafkaConsumer;
import com.demo.kafka_demo.model.TopicName;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class StringKafkaConsumer implements KafkaConsumer<String> {

    @KafkaListener(topics = TopicName.DEMO, groupId = "demoGroup")
    @Override
    public void consume(final String message) {
        log.info("Message received: {}", message);
    }

}