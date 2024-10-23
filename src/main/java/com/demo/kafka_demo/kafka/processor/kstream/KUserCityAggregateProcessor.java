package com.demo.kafka_demo.kafka.processor.kstream;

import com.demo.kafka_demo.model.TopicName;
import com.demo.kafka_demo.model.User;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.HashMap;

@RequiredArgsConstructor
@Component
public class KUserCityAggregateProcessor {

    @Value("${schema-registry.url}")
    private String schemaRegistryUrl;

    private final StreamsBuilder streamBuilder;

    @PostConstruct
    public void runStream() {
        final SpecificAvroSerde<User> userAvroSerde = new SpecificAvroSerde<>();
        userAvroSerde.configure(Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl), false);

        final KStream<String, User> source = streamBuilder.stream(TopicName.USER_TOPIC, Consumed.with(Serdes.String(), userAvroSerde));
        source.print(Printed.<String, User>toSysOut().withLabel(this.getClass().getSimpleName() + " source"));

        final Serde<HashMap<String, Integer>> grouppedCitiesSerde = Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(HashMap.class));
        source
                .groupByKey(Grouped.with(Serdes.String(), userAvroSerde))
//                .windowedBy(SessionWindows.ofInactivityGapAndGrace(Duration.ofSeconds(10), Duration.ofSeconds(5)))
                .aggregate(HashMap::new,
                        (key, user, groupedMap) -> {
                            groupedMap.compute(
                                    user.getAddress().getCity(),
                                    (k, v) -> v == null ? 1 : Integer.parseInt(v.toString()) + 1
                            );
                            return groupedMap;
                        }, Materialized.with(Serdes.String(), grouppedCitiesSerde))
                .toStream()
                .map(KeyValue::new)
                .to(TopicName.CITY_TOPIC, Produced.with(Serdes.String(), grouppedCitiesSerde));
    }

}
