package com.demo.kafka_demo.kafka.consumer;


import com.demo.kafka_demo.kafka.KafkaConsumer;
import com.demo.kafka_demo.model.TopicName;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class EmailConsumer implements KafkaConsumer<String> {

    @KafkaListener(topics = TopicName.EMAIL_TOPIC, groupId = "${spring.kafka.consumer.group-id}", containerFactory = "kafkaListenerContainerFactoryString")
    @Override
    public void consume(String message) {
        log.info("Consumed email: {}", message);
    }

}
