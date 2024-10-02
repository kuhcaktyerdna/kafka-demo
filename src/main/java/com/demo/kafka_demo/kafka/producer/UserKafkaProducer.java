package com.demo.kafka_demo.kafka.producer;

import com.demo.kafka_demo.kafka.KafkaProducer;
import com.demo.kafka_demo.model.TopicName;
import com.demo.kafka_demo.model.User;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

@RequiredArgsConstructor
public class UserKafkaProducer implements KafkaProducer<User> {

    private final KafkaTemplate<String, User> kafkaTemplate;

    @Override
    public KafkaTemplate<String, User> getKafkaTemplate() {
        return kafkaTemplate;
    }

    public void send(User user) {
        send(TopicName.DEMO_OBJECTS, user);
    }

    @Override
    public void send(final String topic, final User user) {
        final Message<User> message = MessageBuilder
                .withPayload(user)
                .setHeader(KafkaHeaders.TOPIC, topic)
                .build();

        kafkaTemplate.send(message);
    }
}
