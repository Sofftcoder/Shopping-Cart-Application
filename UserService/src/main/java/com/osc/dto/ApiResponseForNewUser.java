package com.osc.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ApiResponseForNewUser implements ApiResponse {
    private int code;
    private ListOfNewUserDashboardData dataObject;

    @Override
    public int getcode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public ListOfNewUserDashboardData getDataObject() {
        return dataObject;
    }

    public void setDataObject(ListOfNewUserDashboardData dataObject) {
        this.dataObject = dataObject;
    }
}
