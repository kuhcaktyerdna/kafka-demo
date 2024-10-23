package com.demo.kafka_demo.controller;

import com.demo.kafka_demo.kafka.store.GroupedCitiesStateStore;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.List;

@Slf4j
@RequiredArgsConstructor
@RestController
@RequestMapping("/api/state")
public class StateStoreController {

    private final GroupedCitiesStateStore groupedCitiesStateStore;

    @GetMapping
    public List<KeyValue<String, HashMap<String, Integer>>> getState() {
        final List<KeyValue<String, HashMap<String, Integer>>> stateValues = groupedCitiesStateStore.getAll();
        log.info("{}", stateValues);
        return stateValues;
    }

}
