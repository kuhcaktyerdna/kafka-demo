package com.demo.kafka_demo.kafka.processor;

import com.demo.kafka_demo.model.Address;
import com.demo.kafka_demo.model.User;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;

import java.util.Optional;

public class UserAddressProcessor extends ContextualProcessor<String, User, String, Address> {

    public static final String PROCESSOR_NAME = "ADDRESS_PROCESSOR";
    public static final String SINK_NAME = "ADDRESS_SINK";

    @Override
    public void process(final Record<String, User> recordForProcessing) {
        Optional
                .ofNullable(recordForProcessing.value())
                .map(User::getAddress)
                .ifPresent(address -> {
                    this.context().forward(new Record<>(address.getUuid(), address, System.currentTimeMillis()));
                    this.context().commit();
                });
    }
}
