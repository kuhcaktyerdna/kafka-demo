package com.demo.kafka_demo.kafka.processor.kstream;

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

@RequiredArgsConstructor
@Component
public class KUserEmailProcessor {

    @Value("${schema-registry.url}")
    private String schemaRegistryUrl;

    private final StreamsBuilder streamsBuilder;

    @PostConstruct
    public void runStream() {
        final SpecificAvroSerde<User> userAvroSerde = new SpecificAvroSerde<>();
        userAvroSerde.configure(Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl), false);

        final KStream<String, User> source = streamsBuilder.stream(TopicName.DEMO_OBJECTS, Consumed.with(Serdes.String(), userAvroSerde));
        source.print(Printed.<String, User>toSysOut().withLabel(this.getClass().getSimpleName() + " source"));

        final KStream<String, String> sink = source.mapValues((readOnlyKey, value) -> value.getEmail());
        sink.print(Printed.<String, String>toSysOut().withLabel(this.getClass().getSimpleName() + " sink"));
        sink.to(TopicName.EMAIL_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
    }


}
