package com.osc.config;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;
import java.util.UUID;

@Configuration
public class KafkaConfiguration {

    @Bean
    public KafkaProducer<String, Boolean> kProducerForUserSession() {
        Properties producerProperties = new Properties();
        producerProperties.put("bootstrap.servers", "localhost:9092");
        producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.BooleanSerializer");

        return new KafkaProducer<>(producerProperties);
    }

    @Bean
    public KafkaStreams kafkaStreamObjectUserSession() {
        Properties streamsProperties = new Properties();
        streamsProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-app-" + UUID.randomUUID().toString());
        streamsProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamsProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Boolean().getClass().getName());
        streamsProperties.put(StreamsConfig.STATE_DIR_CONFIG, "C:/Users/Gaurav.s/AppData/Local/Temp/kafka-streams/" + streamsProperties.getProperty(StreamsConfig.APPLICATION_ID_CONFIG));

        StreamsBuilder builder = new StreamsBuilder();
        // Input and output topic
        KTable<String, Boolean> kTable = builder.table("SessionData", Materialized.as("ktable-topic-store"));

        // Build the Kafka Streams topology
        Topology topology = builder.build();

        // Create a KafkaStreams instance
        return new KafkaStreams(topology, streamsProperties);
    }
}