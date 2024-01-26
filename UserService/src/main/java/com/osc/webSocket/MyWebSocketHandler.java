package com.osc.webSocket;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.collection.ISet;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.org.json.JSONObject;
import com.osc.entity.Session;
import com.osc.product.ProductServiceGrpc;
import com.osc.product.SocketRequest;
import com.osc.product.SocketResponse;
import com.osc.repository.SessionRepository;
import com.osc.service.UserServiceImpl;
import com.osc.session.SessionData;
import com.osc.session.SessionDataResponse;
import com.osc.session.SessionServiceGrpc;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MyWebSocketHandler extends TextWebSocketHandler {

    @Autowired
    private SessionRepository sessionRepository;

    @GrpcClient("Product")
    private ProductServiceGrpc.ProductServiceBlockingStub productServiceGrpc;

    @GrpcClient("Session")
    private SessionServiceGrpc.SessionServiceBlockingStub sessionServiceGrpc;

    private HazelcastInstance hazelcastInstance = HazelcastClient.newHazelcastClient();
    public ISet<String> distributedSet = hazelcastInstance.getSet("User Data");

    private WebSocketSession currentSession;
    private Instant lastMessageTime;
    private ScheduledExecutorService executorService;

    @Autowired
    private UserServiceImpl userService;


    public MyWebSocketHandler(){
        startInactivityChecking();
    }

    public void setCurrentSession(WebSocketSession session) {
        this.currentSession = session;
    }

    public void startInactivityChecking() {
        if (executorService == null || executorService.isShutdown()) {
            executorService = Executors.newSingleThreadScheduledExecutor();
            executorService.scheduleAtFixedRate(this::checkInactivity, 0, 5, TimeUnit.SECONDS);
        }
    }


    public void checkInactivity() {
        if (lastMessageTime != null) {
            Instant now = Instant.now();
            long secondsSinceLastMessage = Duration.between(lastMessageTime, now).getSeconds();
            System.out.println(secondsSinceLastMessage);

            if (secondsSinceLastMessage >= 30 && secondsSinceLastMessage < 35) {
                System.out.println("Performing actions for 30s inactivity");
                closeSession();
            }if (secondsSinceLastMessage >= 120) {
                System.out.println("Performing actions for 120s inactivity");
                String userId = distributedSet.iterator().next();
                System.out.println(userId);
                SessionData request = SessionData.newBuilder().setEmail(userId).build();
                SessionDataResponse response = sessionServiceGrpc.logout(request);
                Session session1 = sessionRepository.findById(userId).get();
                System.out.println(session1.getSessionId());
                LocalDateTime localDateTime = LocalDateTime.now();
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss");
                String logouttime = formatter.format(localDateTime);
                session1.setLogoutTime(logouttime);
                sessionRepository.save(session1);
                System.out.println("session closed");
                hazelcastInstance.shutdown();
            }
        }
    }

    public void closeSession() {
        if (currentSession != null) {
            CloseStatus closeStatus = new CloseStatus(CloseStatus.NORMAL.getCode(), "Session closed due to inactivity");
            try {
                currentSession.close(closeStatus);
            } catch (IOException e) {
                e.printStackTrace();
            }
            currentSession = null; // Reset the current session
        }
    }


    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {
        super.afterConnectionEstablished(session);
        setCurrentSession(session);
        startInactivityChecking();
    }

    @Override

    public void handleTextMessage(WebSocketSession session, TextMessage message) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = objectMapper.readTree(message.getPayload());
        String payload = message.getPayload();
        JSONObject existingData = new JSONObject(payload);
        if (!payload.isEmpty() && "ping".equals(jsonNode.get("MT").asText())) {
            System.out.println(payload);
            lastMessageTime = Instant.now();
            String jsonResponse = "{ \"message\": \"hello\" }";
            session.sendMessage(new TextMessage(jsonResponse));
        } else if (!payload.isEmpty() && "2".equals(jsonNode.get("MT").asText()) && jsonNode.has("catId") && jsonNode.has("prodId")) {
            String catId = jsonNode.get("catId").asText();
            String prodId = jsonNode.get("prodId").asText();
            String MT = jsonNode.get("MT").asText();
            SocketRequest request = SocketRequest.newBuilder().setMT(MT).setCatId(catId).setProdId(prodId).build();
            SocketResponse response = this.productServiceGrpc.socketQuery(request);
            session.sendMessage(new TextMessage(response.getResponse()));
        } else if (!payload.isEmpty() && "3".equals(jsonNode.get("MT").asText()) && jsonNode.has("catId") && jsonNode.has("filter")) {
            String catId = jsonNode.get("catId").asText();
            String filter = jsonNode.get("filter").asText();
            String MT = jsonNode.get("MT").asText();
            SocketRequest request = SocketRequest.newBuilder().setMT(MT).setCatId(catId).setFilter(filter).build();
            SocketResponse response = this.productServiceGrpc.socketQuery(request);
            session.sendMessage(new TextMessage(response.getResponse()));
        }
        else if (!payload.isEmpty() && "9".equals(jsonNode.get("MT").asText()) && jsonNode.has("userId") && jsonNode.has("prodId")) {
            String userId = jsonNode.get("userId").asText();
            String prodId = jsonNode.get("prodId").asText();
            String MT = jsonNode.get("MT").asText();
            SocketRequest request = SocketRequest.newBuilder().setMT(MT).setProdId(prodId).setUserId(userId).build();
            SocketResponse response = this.productServiceGrpc.socketQuery(request);
        }
        else if (!payload.isEmpty() && "6".equals(jsonNode.get("MT").asText()) && jsonNode.has("userId")){
            String userId = jsonNode.get("userId").asText();
            String MT = jsonNode.get("MT").asText();
            SocketRequest request = SocketRequest.newBuilder().setMT(MT).setUserId(userId).build();
            SocketResponse response = this.productServiceGrpc.socketQuery(request);
            session.sendMessage(new TextMessage(response.getResponse()));
        }
        else if (!payload.isEmpty() && "8".equals(jsonNode.get("MT").asText()) && jsonNode.has("userId") && jsonNode.has("prodId")){
            String userId = jsonNode.get("userId").asText();
            String prodId = jsonNode.get("prodId").asText();
            String MT = jsonNode.get("MT").asText();
            SocketRequest request = SocketRequest.newBuilder().setMT(MT).setUserId(userId).setProdId(prodId).build();
            SocketResponse response = this.productServiceGrpc.socketQuery(request);
        }
        else if (!payload.isEmpty() && "10".equals(jsonNode.get("MT").asText()) && jsonNode.has("userId") && jsonNode.has("prodId")){
            String userId = jsonNode.get("userId").asText();
            String prodId = jsonNode.get("prodId").asText();
            String MT = jsonNode.get("MT").asText();
            SocketRequest request = SocketRequest.newBuilder().setMT(MT).setUserId(userId).setProdId(prodId).build();
            SocketResponse response = this.productServiceGrpc.socketQuery(request);
        }
        else if (!payload.isEmpty() && "11".equals(jsonNode.get("MT").asText())){
            String MT = jsonNode.get("MT").asText();
            String userId = distributedSet.iterator().next();
            userService.dashBoard(userId);
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) throws Exception {
        System.out.println("session end");
        String userId = distributedSet.iterator().next();
        System.out.println(userId);
        SessionData request = SessionData.newBuilder().setEmail(userId).build();
        SessionDataResponse response = sessionServiceGrpc.logout(request);
        Session session1 = sessionRepository.findById(userId).get();
        System.out.println(session1.getSessionId());
        LocalDateTime localDateTime = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss");
        String logouttime = formatter.format(localDateTime);
        session1.setLogoutTime(logouttime);
        sessionRepository.save(session1);
        distributedSet.remove(userId);
        super.afterConnectionClosed(session, status);
    }

}