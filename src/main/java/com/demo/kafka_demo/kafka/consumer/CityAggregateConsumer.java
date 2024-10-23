package com.demo.kafka_demo.kafka.consumer;

import com.demo.kafka_demo.kafka.KafkaConsumer;
import com.demo.kafka_demo.model.TopicName;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.HashMap;

@Slf4j
@Component
public class CityAggregateConsumer implements KafkaConsumer<HashMap<String, Integer>> {

    @KafkaListener(topics = TopicName.CITY_TOPIC, groupId = "${spring.kafka.consumer.group-id}", containerFactory = "kafkaListenerContainerFactoryHashMap")
    @Override
    public void consume(HashMap<String, Integer> message) {
        log.info("Consumed city aggregation: {}", message);
    }

}
