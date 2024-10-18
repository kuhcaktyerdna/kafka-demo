package com.demo.kafka_demo.kafka.producer;

import com.demo.kafka_demo.kafka.KafkaProducer;
import com.demo.kafka_demo.model.TopicName;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class StringKafkaProducer implements KafkaProducer<String> {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public void send(final String message) {
        send(TopicName.DEMO, message);
    }

    @Override
    public KafkaTemplate<String, String> getKafkaTemplate() {
        return kafkaTemplate;
    }

}
