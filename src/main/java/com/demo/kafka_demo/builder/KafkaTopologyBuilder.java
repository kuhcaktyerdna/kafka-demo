package com.demo.kafka_demo.builder;

import com.demo.kafka_demo.kafka.processor.UserAddressProcessor;
import com.demo.kafka_demo.model.TopicName;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.stereotype.Component;

import java.util.Collections;

@Component
@RequiredArgsConstructor
public class KafkaTopologyBuilder {

    @Value("${schema-registry.url}")
    private String schemaRegistryUrl;

    private final StreamsBuilder streamsBuilder;

    private final KafkaProperties kafkaProperties;

    @PostConstruct
    public void build() {
        final Topology topology = streamsBuilder.build();
        try (final SpecificAvroSerde<SpecificRecord> avroSerde = new SpecificAvroSerde<>();
             final Serde<String> stringSerde = Serdes.serdeFrom(String.class)) {
            avroSerde.configure(Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl), false);

            final String sourceName = "SOURCE";
            topology.addSource(sourceName, stringSerde.deserializer(), avroSerde.deserializer(), TopicName.DEMO_OBJECTS)
                    .addProcessor(UserAddressProcessor.PROCESSOR_NAME, UserAddressProcessor::new, sourceName)
                    .addSink(UserAddressProcessor.SINK_NAME, TopicName.ADDRESS_TOPIC, stringSerde.serializer(), avroSerde.serializer(), UserAddressProcessor.PROCESSOR_NAME);

            try (final KafkaStreams streaming = new KafkaStreams(topology, new StreamsConfig(kafkaProperties.buildStreamsProperties(null)))) {
                streaming.start();
            }
        }
    }

}
