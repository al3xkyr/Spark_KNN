package org.sparkexample.pojo;

import java.io.Serializable;

public class ExtendedPojoRow implements Serializable{
private PojoRow pojo;
private double prediction;

public ExtendedPojoRow (PojoRow pojo , double prediction){
	this.pojo = pojo;
	this.prediction = prediction;
}

public PojoRow getPojo() {
	return pojo;
}

public void setPojo(PojoRow pojo) {
	this.pojo = pojo;
}

public double getPrediction() {
	return prediction;
}

public void setPrediction(double prediction) {
	this.prediction = prediction;
}
}
