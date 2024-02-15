package com.osc.controller;

import com.osc.dto.*;
import com.osc.service.UserServiceImpl;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;

@RestController
@RequestMapping("/user")
public class UserController {
    @Autowired
    private UserServiceImpl userService;

    @PostMapping("/Signup")
    public ResponseEntity<ResponseCode> createUser(@Valid @RequestBody UserInformation user) {
        if (userService.createUser(user)) {
            return userService.sendOtp(user);
        } else {
            ResponseCode responseCode = new ResponseCode(30, null);
            return ResponseEntity.ok(responseCode);
        }
    }

    @PostMapping("/resendotp")
    public ResponseEntity<Code> resendOtp(@Valid @RequestBody Otp user) {
        return userService.validateOtp(user);
    }

    @PostMapping("/Login")
    public ResponseEntity<Data> logIn(@RequestBody Login login1) {
        return userService.logIn(login1);
    }

    @PostMapping("/addUserDetails")
    public ResponseEntity<Code> addUserPassword(@RequestBody Password password) {
        return userService.addUserPassword(password);
    }

    @PostMapping("/validateotp")
    public ResponseEntity<Code> validateOtp(@Valid @RequestBody Otp user) {
        return userService.validateOtp(user);
    }

    @PostMapping("/forgotPassword")
    public ResponseEntity<Code> forgotPassword(@RequestBody UserInformation email) {
        return userService.sendOtpForForgotPassword(email);
    }

    @PostMapping("/validateOTPForForgotPassword")
    public ResponseEntity<Code> validateOtpForForgotPassword(@Valid @RequestBody Otp user) {
        return userService.validateOtpForForgotPassword(user);
    }

    @PostMapping("/logout")
    public ResponseEntity<Code> logout(@RequestBody String userId) {
        return userService.logout(userId);
    }

    @PostMapping("/changePassword")
    public ResponseEntity<Code> changePassword(@RequestBody Password password) {
        return userService.changePassword(password);
    }

    @PostMapping("/dashboard")
    public ResponseEntity<?> dashBoard(@RequestBody String userId) {
        JSONObject jsonObject = new JSONObject(userId);

        // Get the value of the "userId" field
        String userName = jsonObject.getString("userId");
        return ResponseEntity.ok(userService.dashBoard(userName));
    }
}
