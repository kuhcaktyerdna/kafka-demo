package com.demo.kafka_demo.kafka.processor.kstream;

import com.demo.kafka_demo.model.Address;
import com.demo.kafka_demo.model.TopicName;
import com.demo.kafka_demo.model.User;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Collections;

@SuppressWarnings("all")
@RequiredArgsConstructor
@Component
public class KUserAddressProcessor {

    @Value("${schema-registry.url}")
    private String schemaRegistryUrl;

    private final StreamsBuilder streamBuilder;

    @PostConstruct
    public void runStream() {
        final SpecificAvroSerde<User> userAvroSerde = new SpecificAvroSerde<>();
        final SpecificAvroSerde<Address> addressAvroSerde = new SpecificAvroSerde<>();
        userAvroSerde.configure(Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl), false);
        addressAvroSerde.configure(Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl), false);

        final KStream<String, User> source = streamBuilder.stream(TopicName.DEMO_OBJECTS, Consumed.with(Serdes.String(), userAvroSerde));
        source.print(Printed.<String, User>toSysOut().withLabel(this.getClass().getSimpleName() + " source"));

        final KStream<String, Address> sink = source.mapValues((readOnlyKey, value) -> value.getAddress());
        sink.print(Printed.<String, Address>toSysOut().withLabel(this.getClass().getSimpleName() + " sink"));
        sink.to(TopicName.ADDRESS_TOPIC, Produced.with(Serdes.String(), addressAvroSerde));
    }

}
