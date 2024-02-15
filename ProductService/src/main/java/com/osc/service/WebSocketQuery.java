package com.osc.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.osc.ProductServiceApplication;
import com.osc.config.KafkaStreamsConfig;
import com.osc.product.ProductServiceGrpc;
import com.osc.product.SocketResponse;
import io.grpc.stub.StreamObserver;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class WebSocketQuery extends ProductServiceGrpc.ProductServiceImplBase {

    public static Logger logger = LogManager.getLogger(ProductServiceApplication.class);
    @Autowired
    WebSocketQueryResponse webSocketQueryResponse;

    @Autowired
    KafkaStreamsConfig kafkaStreamsConfig;

    /**
     * Used for Initialising the Kafka Streams
     *
     * @param userId
     * @return User Data Present in Ktable.
     */
    // name change
    public String getUserCartDataFromKTable(String userId) {
        KafkaStreams kafkaStreams = kafkaStreamsConfig.kafkaStreamObjectForCartData();
        ReadOnlyKeyValueStore<String, String> ketValueData = kafkaStreams.store(
                StoreQueryParameters.fromNameAndType("CartDetails", QueryableStoreTypes.keyValueStore())
        );
        String listOfValues = ketValueData.get(userId);
        return listOfValues;
    }

    /**
     * Used for handling different Mt request.
     *
     * @param responseObserver
     * @param MT
     * @param ProdId
     * @param CatId
     * @param filter
     * @param userId
     */
    public void mtResponse(StreamObserver<SocketResponse> responseObserver, String MT, String ProdId, String CatId, String filter, String userId){
        try{
        switch (MT) {
            case "2":
                    SocketResponse socketResponse = SocketResponse.newBuilder().setResponse(
                            webSocketQueryResponse.handleMT2(ProdId, CatId, MT,userId)).build();

                    responseObserver.onNext(socketResponse);
                    responseObserver.onCompleted();
                break;
            case "3":
                switch (filter) {
                    case "LH":
                        SocketResponse socket_Response = SocketResponse.newBuilder().setResponse(webSocketQueryResponse.sortPriceLowToHigh(MT, CatId)).build();
                        responseObserver.onNext(socket_Response);
                        responseObserver.onCompleted();
                        break;
                    case "P":
                        SocketResponse socket = SocketResponse.newBuilder().setResponse(webSocketQueryResponse.sortPopularity(MT, CatId)).build();
                        responseObserver.onNext(socket);
                        responseObserver.onCompleted();
                        break;
                    case "HL":
                        SocketResponse newSocketResponse = SocketResponse.newBuilder().setResponse(webSocketQueryResponse.sortPriceHighToLow(MT, CatId)).build();
                        responseObserver.onNext(newSocketResponse);
                        responseObserver.onCompleted();
                        break;
                    case "NF":
                        SocketResponse responseOfSocket = SocketResponse.newBuilder().setResponse(webSocketQueryResponse.sortPopularity(MT, CatId)).build();
                        responseObserver.onNext(responseOfSocket);
                        responseObserver.onCompleted();
                        break;
                    default:
                        break;
                }
                break;
            case "6":
                    // Create a KafkaStreams instance
                    List<Map<String, String>> cartDetails = new ArrayList<>();
                    String listOfValues = getUserCartDataFromKTable(userId);
                        if (listOfValues != null) {
                            ObjectMapper objectMapper = new ObjectMapper();
                            cartDetails = objectMapper.readValue(listOfValues, new TypeReference<>() {
                            });
                        }
                    SocketResponse socket = SocketResponse.newBuilder().setResponse(webSocketQueryResponse.handleMT6(cartDetails, MT)).build();
                    responseObserver.onNext(socket);
                    responseObserver.onCompleted();
                break;
            case "8":
                    String values = getUserCartDataFromKTable(userId);
                    webSocketQueryResponse.handleMT8(values, ProdId, userId);
                    SocketResponse socket_Response = SocketResponse.newBuilder().setResponse("").build();
                    responseObserver.onNext(socket_Response);
                    responseObserver.onCompleted();
                break;
            case "9":
                    String userCartData = getUserCartDataFromKTable(userId);
                    webSocketQueryResponse.handleMT9(userCartData, ProdId, userId);
                    SocketResponse newSocketResponse = SocketResponse.newBuilder().setResponse("").build();
                    responseObserver.onNext(newSocketResponse);
                    responseObserver.onCompleted();
                break;
            case "10":
                String productData = getUserCartDataFromKTable(userId);
                webSocketQueryResponse.handleMT10(productData, ProdId, userId);
                break;
            default:
                break;
        }
        } catch (Exception e) {
            logger.error("An Unexpected Error Occured" + e);
            e.printStackTrace();
        }
    }
}

