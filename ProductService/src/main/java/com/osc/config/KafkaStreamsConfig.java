package com.osc.config;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Properties;
import java.util.UUID;

@Configuration
public class KafkaStreamsConfig{
    @Bean
    public KafkaProducer<String, String> kafkaProducer() {
        Properties producerProperties = new Properties();
        producerProperties.put("bootstrap.servers", "localhost:9092");
        producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer<>(producerProperties);
    }

    @Bean
    public KafkaProducer<String, Integer> kProducer() {
        Properties producerProperties = new Properties();
        producerProperties.put("bootstrap.servers", "localhost:9092");
        producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");

        return new KafkaProducer<>(producerProperties);
    }

    @Bean
    public KafkaStreams kafkaStreamObjectForCartData() {
        StreamsBuilder builder = new StreamsBuilder();
        KTable<String, String> kTable = builder.table("CARTDATA", Materialized.as("CartDetails"));
        Topology topology = builder.build();

        Properties streamsProperties = new Properties();
        streamsProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-appp-" + UUID.randomUUID().toString());
        streamsProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamsProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsProperties.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class.getName());
        streamsProperties.put(StreamsConfig.STATE_DIR_CONFIG, "C:/Users/Gaurav.s/AppData/Local/Temp/kafka-streams/data" + streamsProperties.getProperty(StreamsConfig.APPLICATION_ID_CONFIG));

        // Create a KafkaStreams instance
        return new KafkaStreams(topology, streamsProperties);
    }

    @Bean
    public KafkaStreams kafkaStreamsObjectForProductData() {
        StreamsBuilder builder = new StreamsBuilder();
        KTable<String, Integer> kTable = builder.table("ProductData", Materialized.as("ProductDetails"));
        Topology topology = builder.build();

        Properties streamsProperties = new Properties();
        streamsProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-appp-" + UUID.randomUUID().toString());
        streamsProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamsProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
        streamsProperties.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class.getName());
        streamsProperties.put(StreamsConfig.STATE_DIR_CONFIG, "C:/Users/Gaurav.s/AppData/Local/Temp/kafka-streams/data" + streamsProperties.getProperty(StreamsConfig.APPLICATION_ID_CONFIG));

        // Create a KafkaStreams instance
        return new KafkaStreams(topology, streamsProperties);
    }
}