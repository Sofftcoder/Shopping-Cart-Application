package com.osc.config;

import org.apache.kafka.streams.KafkaStreams;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

@Configuration
public class KafkaStreamsInitializer {

    @Autowired
    private KafkaStreamsConfig kafkaStreamsConfig;

    @PostConstruct
    public void initializeKafkaStreams() {
        kafkaStreamsConfig.kafkaStreamsObjectForProductData().start();
        kafkaStreamsConfig.kafkaStreamObjectForCartData().start();
    }

    @PreDestroy
    public void closeKafkaStreamsForProductData() {
        kafkaStreamsConfig.kafkaStreamsObjectForProductData().close();
        kafkaStreamsConfig.kafkaStreamObjectForCartData().close();

    }
}