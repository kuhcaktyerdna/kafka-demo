package com.demo.kafka_demo.kafka.producer;

import com.demo.kafka_demo.kafka.KafkaProducer;
import com.demo.kafka_demo.model.TopicName;
import com.demo.kafka_demo.model.User;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class UserKafkaProducer implements KafkaProducer<User> {

    private final KafkaTemplate<String, User> kafkaTemplate;

    @Override
    public KafkaTemplate<String, User> getKafkaTemplate() {
        return kafkaTemplate;
    }

    public void send(final User user) {
        send(TopicName.DEMO_OBJECTS, user);
    }

    @Override
    public void send(final String topic, final User user) {
        kafkaTemplate.send(topic, user.getUuid(), user);
    }
}
