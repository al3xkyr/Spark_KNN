package org.sparkexample.pojo;

import org.codehaus.jettison.json.JSONObject;


public class DataFeature {

    JSONObject data;
    Double swn;
    double lable;


    public JSONObject getData() {
        return data;
    }

    public void setData(JSONObject data) {
        this.data = data;
    }

    public Double getSwn() {
        return swn;
    }

    public void setSwn(Double swn) {
        this.swn = swn;
    }

    public double getLable() {
        return lable;
    }

    public void setLable(double lable) {
        this.lable = lable;
    }
}
