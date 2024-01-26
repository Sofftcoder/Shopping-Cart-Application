
package com.osc.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.osc.entity.UserData;
import com.osc.product.*;
import com.osc.repository.ProductRepository;
import com.osc.repository.UserDataRepository;
import io.grpc.stub.StreamObserver;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Component
public class ProductManipulation extends ProductServiceGrpc.ProductServiceImplBase {

    @Autowired
    ProductRepository productRepository;

    @Autowired
    UserDataRepository userDataRepository;

    @Autowired
    @Lazy
    WebSocketQuery webSocketQuery;

    private static final String INPUT_OUTPUT_TOPIC = "ProductData";
    HazelcastInstance hazelcastInstance = HazelcastClient.newHazelcastClient();
    IMap<String, List<Map<String, String>>> limitedMap = hazelcastInstance.getMap("limitedMap");
    private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Properties Configuration for Kafka Ktable.
     *
     * @return
     */
    public Properties propertyConfig() {
        Properties streamsProperties = new Properties();
        streamsProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-appp-" + UUID.randomUUID().toString());
        streamsProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamsProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
        streamsProperties.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class.getName());
        streamsProperties.put(StreamsConfig.STATE_DIR_CONFIG, "C:/Users/Gaurav.s/AppData/Local/Temp/kafka-streams/data" + streamsProperties.getProperty(StreamsConfig.APPLICATION_ID_CONFIG));
        return streamsProperties;
    }

    /**
     * Properties Configuration for Kafka Ktable.
     *
     * @return
     */
    public Properties propertiesConfiguration() {
        Properties streamsProperties = new Properties();
        streamsProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-appp-" + UUID.randomUUID().toString());
        streamsProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamsProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsProperties.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class.getName());
        streamsProperties.put(StreamsConfig.STATE_DIR_CONFIG, "C:/Users/Gaurav.s/AppData/Local/Temp/kafka-streams/data" + streamsProperties.getProperty(StreamsConfig.APPLICATION_ID_CONFIG));
        return streamsProperties;
    }

    /**
     * Kafka Ktable for storing the Products Data.
     */
    public void getAllProduct(List<com.osc.entity.Products> productData) {
        StreamsBuilder builder = new StreamsBuilder();
        // Input and output topic
        KTable<String, Integer> kTable = builder.table(INPUT_OUTPUT_TOPIC, Materialized.as("ktable-topic-storee"));

        // Build the Kafka Streams topology
        Topology topology = builder.build();

        // Create a KafkaStreams instance
        KafkaStreams streams = new KafkaStreams(topology, propertyConfig());

        streams.start();
        for (com.osc.entity.Products data : productData) {
            List<Object> productsList = new ArrayList<>();
            productsList.add(data.getCategoryId());
            productsList.add(data.getProdName());
            productsList.add(data.getProductDescription());
            productsList.add(data.getViewCount());
            productsList.add(data.getProdMarketPrice());

            produceMessage(INPUT_OUTPUT_TOPIC, data.getProductId(), data.getViewCount());
        }

        scheduler.scheduleAtFixedRate(this::updateViewCountData, 0, 30, TimeUnit.SECONDS);
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    /**
     * Kafka Ktable for Storing the User Cart Details.
     */
    public void updateCartInfo() {

        StreamsBuilder builder = new StreamsBuilder();
        // Input and output topic
        KTable<String, String> kTable = builder.table("CARTDATA", Materialized.as("ktable-topic-storeee"));
        // Build the Kafka Streams topology
        Topology topology = builder.build();

        // Create a KafkaStreams instance
        KafkaStreams kafkaStreams = new KafkaStreams(topology, propertiesConfiguration());
        kafkaStreams.start();

        //new Thread(kafkaStreams::start).start();
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }

    public void updateViewCount() {

        StreamsBuilder builder = new StreamsBuilder();
        // Input and output topic
        KTable<String, Integer> kTable = builder.table(INPUT_OUTPUT_TOPIC, Materialized.as("ktable-topic-storee"));

        // Build the Kafka Streams topology
        Topology topology = builder.build();

        // Create a KafkaStreams instance
        KafkaStreams streams = new KafkaStreams(topology, propertyConfig());

        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    /**
     * Updating View Count Data in Every 30 Seconds
     */

    public void updateViewCountData() {
        StreamsBuilder builder = new StreamsBuilder();
        KTable<String, Integer> kTable = builder.table(INPUT_OUTPUT_TOPIC, Materialized.as("ktable-topic-storee"));
        Topology topology = builder.build();
        // Create a KafkaStreams instance
        KafkaStreams streams = new KafkaStreams(topology, propertyConfig());
        streams.start();
        ReadOnlyKeyValueStore<String, Integer> keyValueStore = streams.store(
                StoreQueryParameters.fromNameAndType("ktable-topic-storee", QueryableStoreTypes.keyValueStore())
        );

        keyValueStore.all().forEachRemaining(kv -> {
            String key = kv.key;
            Integer value = kv.value;
            com.osc.entity.Products products = productRepository.findByProductId(key);
            products.setViewCount(value);
            productRepository.save(products);
        });
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    public void kafkaMessageProducer(String topic, String key, String value) {
        Properties producerProperties = new Properties();
        producerProperties.put("bootstrap.servers", "localhost:9092");
        producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties)) {
            producer.send(new ProducerRecord<>(topic, key, value));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void produceMessage(String topic, String key, Integer value) {
        Properties producerProperties = new Properties();
        producerProperties.put("bootstrap.servers", "localhost:9092");
        producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");

        try (KafkaProducer<String, Integer> producer = new KafkaProducer<>(producerProperties)) {
            producer.send(new ProducerRecord<>(topic, key, value));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Updating View Count Of Specific Product in Ktable.
     *
     * @param prodId
     */
    public void updatingViewCountOfProduct(String prodId) {
        StreamsBuilder builder = new StreamsBuilder();
        KTable<String, Integer> kTable = builder.table(INPUT_OUTPUT_TOPIC, Materialized.as("ktable-topic-storee"));
        Topology topology = builder.build();

        // Create a KafkaStreams instance
        KafkaStreams streams = new KafkaStreams(topology, propertyConfig());

        streams.setStateListener((newState, oldState) -> {
            if (newState == KafkaStreams.State.RUNNING) {
                // Access the state store and perform your logic here
                ReadOnlyKeyValueStore<String, Integer> keyValueStore = streams.store(
                        StoreQueryParameters.fromNameAndType("ktable-topic-storee", QueryableStoreTypes.keyValueStore())
                );

                Integer value = keyValueStore.get(prodId);

                if (value != null) {
                    // Produce a message to the input topic
                    produceMessage(INPUT_OUTPUT_TOPIC, prodId, value + 1);
                }
            }
        });
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    /**
     * Grpc method to handle the Web Socket request and response.
     *
     * @param request
     * @param responseObserver
     */
    @Override
    public void socketQuery(SocketRequest request, StreamObserver<SocketResponse> responseObserver) {
        try {
            String MT = request.getMT();
            String ProdId = request.getProdId();
            String CatId = request.getCatId();
            String filter = request.getFilter();
            String userId = request.getUserId();

            webSocketQuery.mtResponse(responseObserver, MT, ProdId, CatId, filter, userId);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Updating User Recently Viewed Data and Cart Product Details On User LogOut.
     *
     * @param email
     * @throws JsonProcessingException
     */
    public void dataUpdationOnLogOut(String email) throws JsonProcessingException {
        StreamsBuilder builder = new StreamsBuilder();
        KTable<String, String> kTable = builder.table("CARTDATA", Materialized.as("ktable-topic-storeee"));
        Topology topology = builder.build();

        // Create a KafkaStreams instance
        KafkaStreams kafkaStreams = new KafkaStreams(topology, propertiesConfiguration());
        kafkaStreams.start();
        while (kafkaStreams.state() != KafkaStreams.State.RUNNING) {
            try {
                Thread.sleep(5000);
            } catch (Exception e) {
                Thread.currentThread().interrupt();
            }
        }
        ReadOnlyKeyValueStore<String, String> keyValueStore = kafkaStreams.store(
                StoreQueryParameters.fromNameAndType("ktable-topic-storeee", QueryableStoreTypes.keyValueStore())
        );
        System.out.println("dfdf" + email);
        if (email != null) {
            String value = keyValueStore.get(email);
            List<Map<String, String>> mapvalues = limitedMap.get(email);
            System.out.println("MapVakues" + mapvalues);
            if (mapvalues != null) {
                String recentViewedProduct = objectMapper.writeValueAsString(mapvalues);
                UserData newUser = new UserData();
                newUser.setUserId(email);
                newUser.setCartDetails(value);
                newUser.setRecentlyViewedDetails(recentViewedProduct);
                userDataRepository.save(newUser);
                System.out.println("hhhhhhh");
            }
        }
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }
}

