package com.demo.kafka_demo.kafka.consumer;

import com.demo.kafka_demo.kafka.KafkaConsumer;
import com.demo.kafka_demo.model.Address;
import com.demo.kafka_demo.model.TopicName;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class AddressConsumer implements KafkaConsumer<Address> {

    @KafkaListener(topics = TopicName.ADDRESS_TOPIC, groupId = "${spring.kafka.consumer.group-id}")
    @Override
    public void consume(final Address address) {
        log.info("Consumed address: {}", address);
    }
}
