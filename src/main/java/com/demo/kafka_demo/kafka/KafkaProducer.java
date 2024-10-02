package com.demo.kafka_demo.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;

public interface KafkaProducer<T> {

    Logger LOGGER = LoggerFactory.getLogger(KafkaProducer.class);

    KafkaTemplate<String, T> getKafkaTemplate();

    default void send(final String topic, final T message) {
        LOGGER.info("Message produced: {} for topic {}", message, topic);
        getKafkaTemplate().send(topic, message);
    }

}
