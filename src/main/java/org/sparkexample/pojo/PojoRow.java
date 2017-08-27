package org.sparkexample.pojo;

import java.io.Serializable;

import org.apache.spark.ml.linalg.Vector;

public class PojoRow implements Serializable{
/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
public Vector features;
public Double label;

public PojoRow(double lable, Vector dv) {
	this.label = lable;
	this.features= dv;
}
public Vector getfeatures() {
	return features;
}
public void setfeatures(Vector features) {
	this.features = features;
}
public Double getLabel() {
	return label;
}
public void setLabel(Double label) {
	this.label = label;
}
}
