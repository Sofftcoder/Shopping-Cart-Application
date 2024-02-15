package com.osc.service;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.osc.cache.*;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

import java.util.ArrayList;
import java.util.List;

@GrpcService
public class CacheService extends CacheServiceGrpc.CacheServiceImplBase{
    String name; String email; String contact; String dob;
    List<String> data = new ArrayList<>();
    HazelcastInstance hazelcastInstance = HazelcastClient.newHazelcastClient();
    IMap<String,List<String>> map = hazelcastInstance.getMap("User Information");
    //Use Clas Object instead of list
    @Override
    public void storeUserData(UserData request, StreamObserver<UserDataResponse> responseObserver) {
         name = request.getName();
         email = request.getEmail();
         contact = request.getContact();
         dob = request.getDob();

        UserDataResponse response = UserDataResponse.newBuilder().setName(email).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();

        data.add(name);
        data.add(contact);
        data.add(dob);

        map.put(email,data);
    }

    @Override
    public void getUserData(CacheData request, StreamObserver<UserDataResponse> responseObserver){
        String email = request.getEmail();
        UserDataResponse response;
        if(map.containsKey(email)){
            List<String> data = map.get(email);
             response = UserDataResponse.newBuilder().setName(data.get(0)).setContact(data.get(1)).setDob(data.get(2)).build();
             map.remove(email);
        }
        else{
             response = UserDataResponse.newBuilder().build();
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
