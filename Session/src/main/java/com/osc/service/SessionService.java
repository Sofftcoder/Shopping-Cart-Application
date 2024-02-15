package com.osc.service;

import com.osc.config.KafkaConfiguration;
import com.osc.session.SessionData;
import com.osc.session.SessionDataResponse;
import com.osc.session.SessionServiceGrpc;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;

@GrpcService
public class SessionService extends SessionServiceGrpc.SessionServiceImplBase {

    @Autowired
    KafkaConfiguration kafkaConfiguration;

    @EventListener(ApplicationReadyEvent.class)
    public void createKafkaTableForSessionCheck() {
        KafkaStreams streams = kafkaConfiguration.kafkaStreamObjectUserSession();
    }

        @Override
        public void sessionCheck(SessionData request, StreamObserver<SessionDataResponse> responseObserver) {
            String email = request.getEmail();
            KafkaStreams streams = kafkaConfiguration.kafkaStreamObjectUserSession();
            ReadOnlyKeyValueStore<String, Boolean> keyValueStore = streams.store(
                    StoreQueryParameters.fromNameAndType("ktable-topic-store", QueryableStoreTypes.keyValueStore())
            );
            String keyToCheck = email;
            Boolean value = keyValueStore.get(keyToCheck);

            if (value == null || !value) {
                // Produce a message to the input topic
                produceMessage("SessionData", email, true);
                SessionDataResponse response = SessionDataResponse.newBuilder().setResponse(false).build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            } else {
                SessionDataResponse response = SessionDataResponse.newBuilder().setResponse(true).build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            }
        }

    private void produceMessage(String topic, String key, Boolean value) {
        KafkaProducer<String, Boolean> producer = kafkaConfiguration.kProducerForUserSession();
            producer.send(new ProducerRecord<>(topic, key, value));
    }

    public void logout(SessionData request, StreamObserver<SessionDataResponse> responseObserver) {
        String email = request.getEmail();
        produceMessage("SessionData",email,false);
        SessionDataResponse response = SessionDataResponse.newBuilder().setResponse(true).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();

    }
}

