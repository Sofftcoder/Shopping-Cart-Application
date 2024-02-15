
package com.osc.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.osc.ProductServiceApplication;
import com.osc.config.HazelcastConfig;
import com.osc.config.KafkaStreamsConfig;
import com.osc.entity.UserData;
import com.osc.product.*;
import com.osc.repository.ProductRepository;
import com.osc.repository.UserDataRepository;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Service
public class ProductManipulation extends ProductServiceGrpc.ProductServiceImplBase {

    public static Logger logger = LogManager.getLogger(ProductServiceApplication.class);

    @Autowired
    ProductRepository productRepository;

    @Autowired
    UserDataRepository userDataRepository;

    @Autowired
    KafkaStreamsConfig kafkaStreamsConfig;

    private static final String INPUT_OUTPUT_TOPIC = "ProductData";
    @Autowired
    HazelcastConfig hazelcastConfig;
    IMap<String, List<Map<String, String>>> userRecentlyViewedData;

    HazelcastInstance hazelcastInstance;

    private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    @PostConstruct
    public void initializeHazelcastInstance() {

        hazelcastInstance = hazelcastConfig.hazelcastInstance();
        userRecentlyViewedData = hazelcastInstance.getMap("UserRecentlyViewedData");
    }

    /**
     * Kafka Streams for Getting the Products Data.
     */
    public void getAllProduct(List<com.osc.entity.Products> productData) {

        // Create a KafkaStreams instance
        KafkaStreams streams = kafkaStreamsConfig.kafkaStreamsObjectForProductData();

        for (com.osc.entity.Products data : productData) {
            KafkaProducer<String, Integer> producer = kafkaStreamsConfig.kProducer();
            producer.send(new ProducerRecord<>(INPUT_OUTPUT_TOPIC, data.getProductId(), data.getViewCount()));
        }

        scheduler.scheduleAtFixedRate(this::updateProductViewCountInDB, 0, 30, TimeUnit.SECONDS);
    }

    public void updateProductViewCountInDB() {
        // Create a KafkaStreams instance
        try {
            KafkaStreams streams = kafkaStreamsConfig.kafkaStreamsObjectForProductData();
            while (streams.state() != KafkaStreams.State.RUNNING) {
                try {
                    Thread.sleep(5000);
                } catch (Exception e) {
                    Thread.currentThread().interrupt();
                }
            }
            ReadOnlyKeyValueStore<String, Integer> keyValueStore = streams.store(
                    StoreQueryParameters.fromNameAndType("ProductDetails", QueryableStoreTypes.keyValueStore())
            );

            keyValueStore.all().forEachRemaining(kv -> {
                String key = kv.key;
                Integer value = kv.value;
                com.osc.entity.Products products = productRepository.findByProductId(key);
                products.setViewCount(value);
                productRepository.save(products);
            });
        } catch (Exception e) {
            logger.error("An Unexpected Error Occurred" + e);
            e.printStackTrace();
        }
    }

    /**
     * Updating View Count Of Specific Product in Ktable.
     *
     * @param prodId
     */
    public void updatingViewCountOfProduct(String prodId) {

        try {
        // Create a KafkaStreams instance
        KafkaStreams streams = kafkaStreamsConfig.kafkaStreamsObjectForProductData();

        // Access the state store and perform your logic here
        ReadOnlyKeyValueStore<String, Integer> keyValueStore = streams.store(
                StoreQueryParameters.fromNameAndType("ProductDetails", QueryableStoreTypes.keyValueStore())
        );
        Integer value = keyValueStore.get(prodId);
        if (value != null) {

            // Produce a message to the input topic
                KafkaProducer<String, Integer> producer = kafkaStreamsConfig.kProducer();
                producer.send(new ProducerRecord<>(INPUT_OUTPUT_TOPIC, prodId, value + 1));
        }
        } catch (Exception e) {
            logger.error("An Unexpected Error Occurred" + e);
            e.printStackTrace();
        }
    }

    /**
     * Updating User Recently Viewed Data and Cart Product Details On User LogOut.
     *
     * @param email
     */
    public void updateDataOnLogout(String email) {
        try {
            // Create a KafkaStreams instance
            KafkaStreams kafkaStreams = kafkaStreamsConfig.kafkaStreamObjectForCartData();
            ReadOnlyKeyValueStore<String, String> keyValueStore = kafkaStreams.store(
                    StoreQueryParameters.fromNameAndType("CartDetails", QueryableStoreTypes.keyValueStore())
            );
            if (email != null) {
                String value = keyValueStore.get(email);
                List<Map<String, String>> mapvalues = userRecentlyViewedData.get(email);
                if (mapvalues != null) {
                    ObjectMapper objectMapper = new ObjectMapper();
                    String recentViewedProduct = objectMapper.writeValueAsString(mapvalues);
                    UserData newUser = new UserData();
                    newUser.setUserId(email);
                    newUser.setCartDetails(value);
                    newUser.setRecentlyViewedDetails(recentViewedProduct);
                    userDataRepository.save(newUser);
                }
            }
        } catch (Exception e) {
            logger.error("An Unexpected Error Occurred" + e);
            e.printStackTrace();
        }
    }
}

