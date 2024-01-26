package com.osc.service;

import com.osc.session.SessionData;
import com.osc.session.SessionDataResponse;
import com.osc.session.SessionServiceGrpc;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Properties;
import java.util.UUID;

@GrpcService
public class SessionService extends SessionServiceGrpc.SessionServiceImplBase {
    private static final String INPUT_OUTPUT_TOPIC = "SessionData";


    private final KafkaStreams streams;

        public SessionService() {
            Properties streamsProperties = new Properties();
            streamsProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-app-" + UUID.randomUUID().toString());
            streamsProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            streamsProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
            streamsProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Boolean().getClass().getName());
            streamsProperties.put(StreamsConfig.STATE_DIR_CONFIG, "C:/Users/Gaurav.s/AppData/Local/Temp/kafka-streams/" + streamsProperties.getProperty(StreamsConfig.APPLICATION_ID_CONFIG));

            StreamsBuilder builder = new StreamsBuilder();
            // Input and output topic
            KTable<String, Boolean> kTable = builder.table(INPUT_OUTPUT_TOPIC, Materialized.as("ktable-topic-store"));

            // Build the Kafka Streams topology
            Topology topology = builder.build();

            // Create a KafkaStreams instance
            this.streams = new KafkaStreams(topology, streamsProperties);

            // Set an uncaught exception handler
            streams.setUncaughtExceptionHandler((thread, throwable) -> {
                System.err.println("Error during KafkaStreams startup: " + throwable.getMessage());
                System.exit(1);
            });

            new Thread(streams::start).start();

            while (streams.state() != KafkaStreams.State.RUNNING) {
                try {
                    Thread.sleep(5000);
                } catch (Exception e) {
                    Thread.currentThread().interrupt();
                }
            }
        }


        @Override
        public void sessionCheck(SessionData request, StreamObserver<SessionDataResponse> responseObserver) {
            String email = request.getEmail();
            System.out.println(email);

            // Sleep for a while to allow the application to process messages
            ReadOnlyKeyValueStore<String, Boolean> keyValueStore = streams.store(
                    StoreQueryParameters.fromNameAndType("ktable-topic-store", QueryableStoreTypes.keyValueStore())
            );
            String keyToCheck = email;
            Boolean value = keyValueStore.get(keyToCheck);

            if (value == null || !value) {
                System.out.println("Key: " + keyToCheck + " is not present in the KTable.");
                // Produce a message to the input topic
                produceMessage(INPUT_OUTPUT_TOPIC, email, true);
                SessionDataResponse response = SessionDataResponse.newBuilder().setResponse(false).build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            } else {
                System.out.println("Key: " + keyToCheck + ", Value: " + value + " is present in the KTable.");
                SessionDataResponse response = SessionDataResponse.newBuilder().setResponse(true).build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            }
        }

    private static void produceMessage(String topic, String key, Boolean value) {
        Properties producerProperties = new Properties();
        producerProperties.put("bootstrap.servers", "localhost:9092");
        producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.BooleanSerializer");

        try (KafkaProducer<String, Boolean> producer = new KafkaProducer<>(producerProperties)) {
            producer.send(new ProducerRecord<>(topic, key, value));
            System.out.println("Produced message: Key=" + key + ", Value=" + value);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void logout(SessionData request, StreamObserver<SessionDataResponse> responseObserver) {
        String email = request.getEmail();
        System.out.println(email);
        produceMessage(INPUT_OUTPUT_TOPIC,email,false);
        SessionDataResponse response = SessionDataResponse.newBuilder().setResponse(true).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();

    }
}

