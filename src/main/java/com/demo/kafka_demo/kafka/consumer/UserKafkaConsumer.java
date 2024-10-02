package com.demo.kafka_demo.kafka.consumer;

import com.demo.kafka_demo.kafka.KafkaConsumer;
import com.demo.kafka_demo.model.TopicName;
import com.demo.kafka_demo.model.User;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class UserKafkaConsumer implements KafkaConsumer<User> {

    @KafkaListener(topics = TopicName.DEMO_OBJECTS, groupId = "demoGroup")
    @Override
    public void consume(User message) {
        log.info("Consumed message: {}", message);
    }

}
