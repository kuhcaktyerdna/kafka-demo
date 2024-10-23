package com.demo.kafka_demo.config;

import com.demo.kafka_demo.model.TopicName;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    @Value(value = "${spring.kafka.consumer.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public NewTopic demoTopic() {
        return TopicBuilder
                .name(TopicName.DEMO)
                .build();
    }

    @Bean
    public NewTopic usersTopic() {
        return TopicBuilder
                .name(TopicName.USER_TOPIC)
                .partitions(2)
                .build();
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactoryString() {
        final ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        final ConsumerFactory<String, String> consumerFactory = generateFactory(new StringDeserializer(), new StringDeserializer());
        factory.setConsumerFactory(consumerFactory);
        return factory;
    }

    @Bean
    public <K, V> ConcurrentKafkaListenerContainerFactory<String, HashMap<K, V>> kafkaListenerContainerFactoryHashMap() {
        final ConcurrentKafkaListenerContainerFactory<String, HashMap<K, V>> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        final ConsumerFactory<String, HashMap<K, V>> consumerFactory =
                generateFactory(new StringDeserializer(), new JsonDeserializer<>(HashMap.class, false));
        factory.setConsumerFactory(consumerFactory);
        return factory;
    }

    private <K, V> ConsumerFactory<K, V> generateFactory(final Deserializer<K> keyDeserializer,
                                                         final Deserializer<V> valueDeserializer) {
        final Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        return new DefaultKafkaConsumerFactory<>(props, keyDeserializer, valueDeserializer);
    }

}
