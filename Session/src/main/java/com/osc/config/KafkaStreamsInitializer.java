package com.osc.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

@Configuration
public class KafkaStreamsInitializer {

    @Autowired
    private KafkaConfiguration kafkaConfiguration;

    @PostConstruct
    public void initializeKafkaStreams() {
        kafkaConfiguration.kafkaStreamObjectUserSession().start();
    }

    @PreDestroy
    public void closeKafkaStreamsForProductData() {
        kafkaConfiguration.kafkaStreamObjectUserSession().close();

    }
}