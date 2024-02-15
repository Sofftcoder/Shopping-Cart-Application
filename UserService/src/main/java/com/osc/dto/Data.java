package com.osc.dto;

public class Data {
    private int code;
    private Session1 dataObject;

    public Data(int code){
        this.code = code;
    }
    public Data(int code,DataObject dataObject) {
        this.code = code;
        if (dataObject==null){
            this.dataObject=null;
        } else{
            this.dataObject=new Session1(dataObject.getSessionId(),dataObject.getName());
        }

    }

    public Data() {
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public Session1 getDataObject() {
        return dataObject;
    }

    public void setDataObject(Session1 dataObject) {
        this.dataObject = dataObject;
    }
}
