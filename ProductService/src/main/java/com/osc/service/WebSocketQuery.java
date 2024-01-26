package com.osc.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.osc.product.ProductServiceGrpc;
import com.osc.product.SocketResponse;
import io.grpc.stub.StreamObserver;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class WebSocketQuery extends ProductServiceGrpc.ProductServiceImplBase {
    @Autowired
    WebSocketData webSocketData;

    private KafkaStreams kafkaStreams;

    private StreamsBuilder streamsBuilder = new StreamsBuilder();
    ObjectMapper objectMapper = new ObjectMapper();

    KTable<String, Integer> kTable = streamsBuilder.table("CARTDATA", Materialized.as("ktable-topic-storeee"));
    Topology topology = streamsBuilder.build();

    @Autowired
    ProductManipulation productManipulation;

    /**
     * Used for Intialising the Kafka Streams
     * @param userId
     * @return User Data Present in Ktable.
     */
    public String initializeKafkaStreams(String userId) {
        kafkaStreams = new KafkaStreams(topology, productManipulation.propertiesConfiguration());
        kafkaStreams.start();
        while (kafkaStreams.state() != KafkaStreams.State.RUNNING) {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        ReadOnlyKeyValueStore<String, String> ketValueData = kafkaStreams.store(
                StoreQueryParameters.fromNameAndType("ktable-topic-storeee", QueryableStoreTypes.keyValueStore())
        );
        String listOfValues = ketValueData.get(userId);
        return listOfValues;
    }

    /**
     * Used for handling different Mt request.
     * @param responseObserver
     * @param MT
     * @param ProdId
     * @param CatId
     * @param filter
     * @param userId
     * @throws JsonProcessingException
     */
    public void mtResponse(StreamObserver<SocketResponse> responseObserver, String MT, String ProdId, String CatId, String filter, String userId) throws JsonProcessingException {
        switch (MT) {
            case "2":
                try {
                    SocketResponse socketResponse = SocketResponse.newBuilder().setResponse(
                            webSocketData.handleMT2(ProdId, CatId, userId, MT)).build();

                    responseObserver.onNext(socketResponse);
                    responseObserver.onCompleted();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                break;
            case "3":
                switch (filter) {
                    case "LH":
                        try {
                            SocketResponse socket_Response = SocketResponse.newBuilder().setResponse(webSocketData.handleFilterLH(MT, CatId)).build();
                            responseObserver.onNext(socket_Response);
                            responseObserver.onCompleted();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        break;
                    case "P":
                        try {
                            SocketResponse socket = SocketResponse.newBuilder().setResponse(webSocketData.handleFilterP(MT, CatId)).build();
                            responseObserver.onNext(socket);
                            responseObserver.onCompleted();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        break;
                    case "HL":
                        try {
                            SocketResponse newSocketResponse = SocketResponse.newBuilder().setResponse(webSocketData.handleFilterHL(MT, CatId)).build();
                            responseObserver.onNext(newSocketResponse);
                            responseObserver.onCompleted();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        break;
                    case "NF":
                        try {
                            SocketResponse responseOfSocket = SocketResponse.newBuilder().setResponse(webSocketData.handleFilterNF(MT, CatId)).build();
                            responseObserver.onNext(responseOfSocket);
                            responseObserver.onCompleted();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        break;
                    default:
                        break;
                }
                break;
            case "6":
                try {
                    // Create a KafkaStreams instance
                    List<Map<String, String>> cartDetails = new ArrayList<>();
                    String listOfValues = initializeKafkaStreams(userId);
                    try {
                        if (listOfValues != null) {
                            cartDetails = objectMapper.readValue(listOfValues, new TypeReference<>() {
                            });
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    SocketResponse socket = SocketResponse.newBuilder().setResponse(webSocketData.handleMT6(cartDetails, MT)).build();
                    responseObserver.onNext(socket);
                    responseObserver.onCompleted();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                break;
            case "8":
                try {
                    String values = initializeKafkaStreams(userId);
                    webSocketData.handleMT8(MT, values, ProdId, userId);
                    SocketResponse socket_Response = SocketResponse.newBuilder().setResponse("").build();
                    responseObserver.onNext(socket_Response);
                    responseObserver.onCompleted();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                break;
            case "9":
                try {
                    String values = initializeKafkaStreams(userId);
                    webSocketData.handleMT9(values, ProdId, userId);
                    SocketResponse newSocketResponse = SocketResponse.newBuilder().setResponse("").build();
                    responseObserver.onNext(newSocketResponse);
                    responseObserver.onCompleted();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                break;
            case "10":
                String productData = initializeKafkaStreams(userId);
                webSocketData.handleMT10(productData, ProdId, userId);
                break;
            default:
                break;
        }
    }
}

