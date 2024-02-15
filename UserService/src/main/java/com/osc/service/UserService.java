package com.osc.service;

import com.osc.dto.*;
import org.springframework.http.ResponseEntity;

public interface UserService {
    boolean createUser(UserInformation user);

    ResponseEntity<Code> validateOtp(Otp user);

    ResponseEntity<Data> logIn(Login login);

    ResponseEntity<Code> sendOtpForForgotPassword(UserInformation user);

    ResponseEntity<ResponseCode> sendOtp(UserInformation user);

    ResponseEntity<Code> addUserPassword(Password password);

    ResponseEntity<Code> validateOtpForForgotPassword(Otp user);

    boolean checkUserExits(String email);

    ResponseEntity<Code> logout(String email);

    ResponseEntity<Code> changePassword(Password password);

    ApiResponse dashBoard(String userId);

}
