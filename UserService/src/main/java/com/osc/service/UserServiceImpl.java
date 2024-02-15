package com.osc.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.osc.cache.CacheData;
import com.osc.cache.CacheServiceGrpc;
import com.osc.cache.UserData;
import com.osc.cache.UserDataResponse;
import com.osc.dto.*;
import com.osc.entity.Session;
import com.osc.entity.User;
import com.osc.notification.EmailNotification;
import com.osc.notification.NotificationRequest;
import com.osc.notification.NotificationResponse;
import com.osc.notification.NotificationServiceGrpc;
import com.osc.product.ProductData;
import com.osc.product.ProductDataResponse;
import com.osc.product.ProductServiceGrpc;
import com.osc.repository.SessionRepository;
import com.osc.repository.UserRepository;
import com.osc.session.SessionData;
import com.osc.session.SessionDataResponse;
import com.osc.session.SessionServiceGrpc;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.servlet.http.HttpSession;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class UserServiceImpl extends UserDashBoardDetails implements UserService {

    UserDataResponse response;
    @GrpcClient("Product")
    private ProductServiceGrpc.ProductServiceBlockingStub productServiceGrpc;
    @GrpcClient("Cache")
    private CacheServiceGrpc.CacheServiceBlockingStub cacheServiceGrpc;
    @GrpcClient("Notification")
    private NotificationServiceGrpc.NotificationServiceBlockingStub notificationServiceBlockingStub;
    @GrpcClient("Session")
    private SessionServiceGrpc.SessionServiceBlockingStub sessionServiceGrpc;
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    @Autowired
    private UserRepository userRepository;
    @Autowired
    private SessionRepository sessionRepository;
    @Autowired
    private KafkaTemplate<String, String> template;

    @Override
    public boolean createUser(UserInformation user) {
        if (checkUserExits(user.getEmail())) {
            return false;
        } else {
            // Created a gRPC request to store the data into Hazelcast present in Cache Service
            UserData request = UserData.newBuilder().setName(user.getName()).setEmail(user.getEmail()).setContact(user.getContact()).setDob(user.getDOB()).build();
            response = this.cacheServiceGrpc.storeUserData(request);
            return true;
        }
    }


    @Override
    public ResponseEntity<Code> validateOtp(Otp user) {
        try {
            //Created a GRPC call to Notification Service to provide the User entered otp and checking for the response
            NotificationRequest request = NotificationRequest.newBuilder().setEmail(user.getUserId()).setOtp(user.getOTP()).build();
            NotificationResponse response = this.notificationServiceBlockingStub.storeOtp(request);


            //get the otp from notification service and verify here only
            if (response.getValid().equals("Valid")) {
                Code responseCode = new Code(500);
                return ResponseEntity.ok(responseCode);
            } else if (response.getValid().equals("InValid")) {
                Code responseCode = new Code(301);
                return ResponseEntity.ok(responseCode);
            } else if (response.getValid().equals("wrong")) {
                Code responseCode = new Code(502);
                return ResponseEntity.ok(responseCode);
            } else {
                Code responseCode = new Code(1999);
                return ResponseEntity.ok(responseCode);
            }
        } catch (Exception e) {
            Code responseCode = new Code(0);
            return ResponseEntity.ok(responseCode);
        }
    }


    @Override
    public ResponseEntity<Code> validateOtpForForgotPassword(Otp user) {

        //Created a GRPC call to Notification Service to provide the User entered otp and checking for the response
        NotificationRequest request = NotificationRequest.newBuilder().setEmail(user.getUserId()).setOtp(user.getOTP()).build();
        NotificationResponse response = this.notificationServiceBlockingStub.storeOtp(request);
        //change the otp logic here also
        if (response.getValid().equals("Valid")) {
            Code responseCode = new Code(200);
            return ResponseEntity.ok(responseCode);
        } else if (response.getValid().equals("InValid")) {
            Code responseCode = new Code(301);
            return ResponseEntity.ok(responseCode);
        } else {
            Code responseCode = new Code(199);
            return ResponseEntity.ok(responseCode);
        }

    }

    public ResponseEntity<Code> sendOtpForForgotPassword(UserInformation user) {
        String email = user.getEmail();
        if (checkUserExits(email)) {
            //Created a GRPC call to Notification Service to send the otp to use for changing the password
            EmailNotification request = EmailNotification.newBuilder().setEmail(email).build();
            NotificationResponse response = this.notificationServiceBlockingStub.forgotPassword(request);

            Code responseCode = new Code(200);
            return ResponseEntity.ok(responseCode);
        } else {
            Code responseCode = new Code(199);
            return ResponseEntity.ok(responseCode);
        }
    }

    @Override
    public ResponseEntity<Data> logIn(Login login) {
        try {
            String email = login.getUserId();
            int atIndex = email.indexOf('@');
            String result = atIndex != -1 ? email.substring(0, atIndex) : email;
            String id = result + login.getLoginDevice();
            if (checkUserExits(login.getUserId())) {//
                String password = userRepository.findPassword(login.getUserId());
                if (password.equals(login.getPassword())) {
                    //Created a GRPC call to Session Service to provide the User+loginDevice data and checking for the response
                    SessionData request = SessionData.newBuilder().setEmail(id).build();
                    SessionDataResponse response = sessionServiceGrpc.sessionCheck(request);

                    if (response.getResponse()) {
                        Data responseCode = new Data(204, null);
                        return ResponseEntity.ok(responseCode);
                    } else {
                        //rows should be different
                        ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.currentRequestAttributes();
                        HttpSession httpSession = attributes.getRequest().getSession();
                        Session session = new Session();
                        String sessionId = httpSession.getId();
                        session.setSessionId(httpSession.getId());
                        LocalDateTime localDateTime = LocalDateTime.now();
                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss");
                        String loginTime = formatter.format(localDateTime);
                        session.setUserId(login.getUserId());
                        session.setDeviceId(login.getLoginDevice());
                        session.setLoginTime(loginTime);
                        sessionRepository.save(session);
                        Data responseCode = new Data(200, new DataObject(sessionId, userRepository.findName(login.getUserId())));
                        return ResponseEntity.ok(responseCode);
                    }
                } else {
                    Data responseCode = new Data(202, null);
                    return ResponseEntity.ok(responseCode);
                }
            } else {
                Data responseCode = new Data(201, null);
                return ResponseEntity.ok(responseCode);
            }
        } catch (Exception e) {
            Data responseCode = new Data(0, null);
            return ResponseEntity.ok(responseCode);
        }
    }

    @Override
    public ResponseEntity<Code> logout(String userId) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode jsonNode = mapper.readTree(userId);
            String user = jsonNode.get("userId").asText().replaceAll("^\"|\"$", "");
            System.out.println("User" + user);
            System.out.println(userId);
            int atIndex = user.indexOf('@');
            String result = atIndex != -1 ? user.substring(0, atIndex) : user;
            String id = result + "Web";

            //Created a GRPC call to Session Service to provide the User+loginDevice data and checking for the response
            SessionData request = SessionData.newBuilder().setEmail(id).build();
            SessionDataResponse response = sessionServiceGrpc.logout(request);

            if (response.getResponse()) {
                Session session = sessionRepository.findById(user).get();
                LocalDateTime localDateTime = LocalDateTime.now();
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss");
                String timeout = formatter.format(localDateTime);
                session.setLogoutTime(timeout);
                sessionRepository.save(session);
                Code responseCode = new Code(200);
                this.template.send("data", user);
                return ResponseEntity.ok(responseCode);
            } else {
                Code responseCode = new Code(0);
                return ResponseEntity.ok(responseCode);
            }
        } catch (Exception e) {
            Code responseCode = new Code(0);
            return ResponseEntity.ok(responseCode);
        }
    }

    @Override
    public ResponseEntity<Code> addUserPassword(Password password) {
        try {
            //Created a GRPC call to Cache Service to provide the userId and get all data in response
            CacheData request = CacheData.newBuilder().setEmail(password.getUserId()).build();
            UserDataResponse response = cacheServiceGrpc.getUserData(request);  // method name should be correct

            //Setting all Data to User
            User user = new User(response.getName(), password.getUserId(), response.getContact(),
                    response.getDob(), password.getUserId(), password.getPassword());
            userRepository.save(user);
            Code responseCode = new Code(200);
            return ResponseEntity.ok(responseCode);
        } catch (Exception e) {
            Code responseCode = new Code(0);
            return ResponseEntity.ok(responseCode);
        }
    }

    public boolean checkUserExits(String email) {
        System.out.println(email);
        System.out.println(userRepository.existsByEmail(email));
        return userRepository.existsByEmail(email);
    }

    @Override
    public ResponseEntity<Code> changePassword(Password password) {
        if (checkUserExits(password.getUserId())) {
            userRepository.changePassword(password.getUserId(), password.getPassword());
            Code responseCode = new Code(200);
            return ResponseEntity.ok(responseCode);
        } else {
            Code responseCode = new Code(199);
            return ResponseEntity.ok(responseCode);
        }


    }

    public ResponseEntity<ResponseCode> sendOtp(UserInformation user) {
        try {
            if (user != null) {
                String email = user.getEmail();
                String name = user.getName();
                Map<String, String> map = new HashMap<>();
                map.put("name", name);
                map.put("email", email);
                ObjectMapper objectMapper = new ObjectMapper();
                String userDetails = objectMapper.writeValueAsString(map);
                this.kafkaTemplate.send("message", userDetails);
                DataObject data = new DataObject(user.getEmail());
                ResponseCode responseCode = new ResponseCode(200, data);
                return ResponseEntity.ok(responseCode);
            } else {
                ResponseCode responseCode = new ResponseCode(220, null);
                return ResponseEntity.ok(responseCode);
            }
        } catch (Exception e) {
            ResponseCode responseCode = new ResponseCode(0, null);
            return ResponseEntity.ok(responseCode);
        }

    }

    @Override
    public ApiResponse dashBoard(String userId) {
        //Created a GRPC call to Product Service to provide the data and checking for the response
        ProductData request = ProductData.newBuilder().setRequest(userId).build();
        ProductDataResponse response = this.productServiceGrpc.getDashBoardData(request);

        /*Existing User DashBoard Data */
        if (!response.getValue()) {
            com.osc.product.ListOfUserData grpcResponse = response.getListOfUserData();
            List<ExistingUserDashboardData> userDashboardData = new ArrayList<>();
            existingUserDashboard(userId, grpcResponse, userDashboardData);

            ListOfExistingDashboardData listOfExistingDashboardData = new ListOfExistingDashboardData();
            listOfExistingDashboardData.setData(userDashboardData);
            ApiResponseForExistingUser apiResponseForExistingUser = new ApiResponseForExistingUser();
            apiResponseForExistingUser.setCode(200);
            apiResponseForExistingUser.setDataObject(listOfExistingDashboardData);
            return apiResponseForExistingUser;

        } else {
            /*New User DashBoard Data*/
            ListOfNewUserDashboardData listOfNewUserDashboardData = new ListOfNewUserDashboardData();

            com.osc.product.ListOfUserData grpcResponse = response.getListOfUserData();
            List<NewUserDashboardData> userDashboardData = new ArrayList<>();
            newUserDashboard(grpcResponse, userDashboardData);
            listOfNewUserDashboardData.setData(userDashboardData);
            ApiResponseForNewUser apiResponseForNewUser = new ApiResponseForNewUser();
            apiResponseForNewUser.setCode(200);
            apiResponseForNewUser.setDataObject(listOfNewUserDashboardData);
            return apiResponseForNewUser;
        }
    }
}