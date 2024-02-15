package com.osc.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
public class ApiResponseForExistingUser implements ApiResponse {
    private int code;
    private ListOfExistingDashboardData dataObject;

    @Override
    public int getcode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public ListOfExistingDashboardData getDataObject() {
        return dataObject;
    }

    public void setDataObject(ListOfExistingDashboardData dataObject) {
        this.dataObject = dataObject;
    }

}
