package org.sparkexample.pojo;

import java.io.Serializable;

public class ExtendedPojoRow implements Serializable{
private PojoRow pojo;
private Double prediction;

public ExtendedPojoRow (PojoRow pojo , Double prediction){
	this.pojo = pojo;
	this.prediction = prediction;
}

public PojoRow getPojo() {
	return pojo;
}

public void setPojo(PojoRow pojo) {
	this.pojo = pojo;
}

public Double getPrediction() {
	return prediction;
}

public void setPrediction(double prediction) {
	this.prediction = prediction;
}
}
