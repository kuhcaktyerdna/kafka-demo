package com.demo.kafka_demo.kafka.store;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@RequiredArgsConstructor
@Component
public class GroupedCitiesStateStore {

    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    public List<KeyValue<String, HashMap<String, Integer>>> getAll() {
        try (final KeyValueIterator<String, HashMap<String, Integer>> keyValueIterator = init().all()) {
            final List<KeyValue<String, HashMap<String, Integer>>> list = new ArrayList<>();
            while (keyValueIterator.hasNext()) {
                list.add(keyValueIterator.next());
            }

            return list;
        }
    }

    public ReadOnlyKeyValueStore<String, HashMap<String, Integer>> init() {
        final KafkaStreams kafkaStreams = streamsBuilderFactoryBean.getKafkaStreams();
        return kafkaStreams.store(StoreQueryParameters
                .fromNameAndType("groupedCities", QueryableStoreTypes.keyValueStore()));
    }


}
