package com.osc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.osc.service.ProductManipulation;
import com.osc.service.ProductService;
import com.osc.service.WebSocketData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;

@Configuration
public class KafkaConfig {

    @Autowired
    ProductManipulation productManipulation;

    /**
     * Kafka Subscriber invoked on User gets logged out.
     *
     * @param email
     * @throws JsonProcessingException
     */
    @KafkaListener(topics = "data", groupId = "group-1")
    public void notification(String email) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        productManipulation.dataUpdationOnLogOut(email.replaceAll("^\"|\"$", ""));
    }
}