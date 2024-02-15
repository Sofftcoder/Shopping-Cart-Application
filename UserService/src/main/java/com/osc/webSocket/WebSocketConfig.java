package com.osc.webSocket;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.collection.IList;
import com.hazelcast.collection.ISet;
import com.hazelcast.core.HazelcastInstance;
import com.osc.dto.Details;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;
import org.springframework.web.socket.server.HandshakeInterceptor;
import java.util.Base64;
import java.util.Map;

@Configuration
@EnableWebSocket
public class WebSocketConfig implements WebSocketConfigurer {

    @Bean
    public WebSocketHandler myWebSocketHandler() {
        return new MyWebSocketHandler();
    }


    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(myWebSocketHandler(),"/").setAllowedOrigins("*").addInterceptors(new HttpHandshakeInterceptor());
    }

    private static class HttpHandshakeInterceptor implements HandshakeInterceptor {

        HazelcastInstance hazelcastInstance = HazelcastClient.newHazelcastClient();
        IList<String> distributedList = hazelcastInstance.getList("User Data");
        Details data = new Details();
        @Override
        public boolean beforeHandshake(ServerHttpRequest request, ServerHttpResponse response, WebSocketHandler wsHandler, Map<String, Object> attributes) throws Exception {
            String headers = request.getHeaders().get("sec-websocket-protocol").toString();
            String [] value = headers.substring(1, headers.length() - 1).split(",\\s*");
            String encodedUserId = value[1].trim();
            byte[] decodedBytes = Base64.getUrlDecoder().decode(encodedUserId);
            String userId = new String(decodedBytes);
             if("OSC-WebSocket-Protocol".equals(value[0])){
                try {
                    data.setUserId(userId);
                    data.setLoginDevice(value[2]);
                    distributedList.add(userId);
                    response.getHeaders().add("sec-websocket-protocol", value[0]);
                    return true;
                }

                catch (Exception e){
                    return false;
                }
            }
            else {
                return false;
            }
        }

        @Override
        public void afterHandshake(ServerHttpRequest request, ServerHttpResponse response, WebSocketHandler wsHandler, Exception exception) {

        }
    }
}
