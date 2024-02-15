package com.osc.service;

import com.osc.notification.EmailNotification;
import com.osc.notification.NotificationRequest;
import com.osc.notification.NotificationResponse;
import com.osc.notification.NotificationServiceGrpc;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import java.util.Random;

@GrpcService
public class NotificationService extends NotificationServiceGrpc.NotificationServiceImplBase
{
    @Autowired
    private JavaMailSender mailSender;
    int max=999999,min=100000;
    Random random = new Random();
    String random_int = String.valueOf(random.nextInt(max-min+1)+min);

    String otp;
    public String userId;
    int count = 1;

    public void sendMail(String email) {
        this.userId = email;
        otp = random_int;
        SimpleMailMessage simpleMailMessage = new SimpleMailMessage();
        simpleMailMessage.setFrom("Gaurav");
        simpleMailMessage.setTo(email);
        simpleMailMessage.setSubject("OROSOFT Solutions Pvt ltd.");
        simpleMailMessage.setText("Name :"+ email + "\nYOUR OTP IS :  "+random_int );
        this.mailSender.send(simpleMailMessage);
    }

    @Override
    public void storeOtp(NotificationRequest request, StreamObserver<NotificationResponse> responseObserver) {
        String newOtp = request.getOtp();
        String userEmail = request.getEmail();
        NotificationResponse response;
            if(newOtp.equals(otp)){
                response = NotificationResponse.newBuilder().setValid("Valid").build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            }
            else{
                if(count == 3){
                    response = NotificationResponse.newBuilder().setValid("InValid").build();
                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                    random = new Random();
                    random_int = String.valueOf(random.nextInt(max-min+1)+min);
                    otp = random_int;
                    SimpleMailMessage simpleMailMessage = new SimpleMailMessage();
                    simpleMailMessage.setFrom("Gaurav");
                    simpleMailMessage.setTo(userEmail);
                    simpleMailMessage.setSubject("OROSOFT Solutions Pvt ltd.");
                    simpleMailMessage.setText("User_Id :"+ userEmail + "\nYOUR OTP IS :  "+random_int );
                    this.mailSender.send(simpleMailMessage);
                    count = 1;
                }
                else{
                    response = NotificationResponse.newBuilder().setValid("wrong").build();
                    count++;
                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                }
            }
    }

    public void welcomeMessage(String name,String email){
        SimpleMailMessage simpleMailMessage = new SimpleMailMessage();
        simpleMailMessage.setFrom("Gaurav");
        simpleMailMessage.setTo(email);
        simpleMailMessage.setSubject("OROSOFT Solutions Pvt ltd.");
        simpleMailMessage.setText("Hello "+ name + "\nWelcome to OroSoft, your gateway to tailored ERP solutions for the Precious Metals Industry. With over 13 years of expertise, we're here to enhance your business performance and profitability through customized software solutions. Let's embark on a journey of success together!");
        this.mailSender.send(simpleMailMessage);
    }

    @Override
    public void forgotPassword(EmailNotification request, StreamObserver<NotificationResponse> responseObserver) {
        random = new Random();
        random_int = String.valueOf(random.nextInt(max-min+1)+min);
        otp = random_int;
        SimpleMailMessage simpleMailMessage = new SimpleMailMessage();
        simpleMailMessage.setFrom("Gaurav");
        simpleMailMessage.setTo(request.getEmail());
        simpleMailMessage.setSubject("OROSOFT Solutions Pvt ltd.");
        simpleMailMessage.setText("YOUR OTP IS :  "+random_int );
        this.mailSender.send(simpleMailMessage);
        NotificationResponse response = NotificationResponse.newBuilder().build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }}
