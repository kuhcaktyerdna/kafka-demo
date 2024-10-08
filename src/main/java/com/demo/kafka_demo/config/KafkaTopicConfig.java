package com.demo.kafka_demo.config;

import com.demo.kafka_demo.model.TopicName;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic demoTopic() {
        return TopicBuilder
                .name(TopicName.DEMO)
                .build();
    }

    @Bean
    public NewTopic demoObectsTopic() {
        return TopicBuilder
                .name(TopicName.DEMO_OBJECTS)
                .build();
    }

}
