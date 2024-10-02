package com.demo.kafka_demo.kafka;

public interface KafkaConsumer<T> {

    void consume(final T message);

}
